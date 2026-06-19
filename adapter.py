"""
VKontakte (VK) platform adapter for Hermes Agent.

Plugin-based gateway adapter that connects to a VK community bot and relays
messages to/from the Hermes agent via Long Poll API.

Configuration via environment variables:
    VK_TOKEN              VK Community API token with messages permission
    VK_ALLOWED_USERS      Comma-separated VK user IDs allowed to chat
    VK_HOME_CHANNEL       VK peer_id for cron/notification delivery
    VK_API_VERSION        VK API version (default: 5.199)
    VK_ALLOW_ALL_USERS    Allow all users (dev/test only)
    VK_POLLING_TIMEOUT    Long Poll timeout in seconds (default: 25)

Install dependencies:
    pip install vk_api aiohttp
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import random
import threading
import time
import urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional dependencies: vk_api
# ---------------------------------------------------------------------------

try:
    import vk_api
    from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType

    VK_AVAILABLE = True
except ImportError:
    VK_AVAILABLE = False
    vk_api = None  # type: ignore
    VkBotLongPoll = None  # type: ignore
    VkBotEventType = None  # type: ignore

# ---------------------------------------------------------------------------
# Hermes gateway imports (available at runtime inside the installed agent)
# ---------------------------------------------------------------------------

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_audio_from_bytes,
    cache_document_from_bytes,
    cache_image_from_bytes,
)
from gateway.config import Platform, PlatformConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# VK group-chat peer_ids start at this offset
_CHAT_PEER_OFFSET = 2_000_000_000

MAX_MESSAGE_LENGTH = 4096  # VK message character limit


# ---------------------------------------------------------------------------
# Dependency check (called by plugin system)
# ---------------------------------------------------------------------------


def check_vkontakte_requirements() -> bool:
    """Check whether vk_api is available; lazy-install if not."""
    global VK_AVAILABLE, vk_api, VkBotLongPoll, VkBotEventType

    if VK_AVAILABLE:
        return True

    try:
        from tools.lazy_deps import ensure as _ensure  # type: ignore

        _ensure("platform.vkontakte", prompt=False)
    except Exception:
        pass

    try:
        import vk_api as _vk_api
        from vk_api.bot_longpoll import VkBotLongPoll as _LP, VkBotEventType as _ET

        vk_api = _vk_api
        VkBotLongPoll = _LP
        VkBotEventType = _ET
        VK_AVAILABLE = True
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class VKPlatformAdapter(BasePlatformAdapter):
    """VKontakte community bot adapter using the VK Bot Long Poll API.

    Receives messages via VK Bot Long Poll and sends replies through
    the VK Messages API. Supports both DMs and multi-user conversations.
    """

    def __init__(self, config: PlatformConfig, **kwargs: Any) -> None:
        super().__init__(config=config, platform=Platform("vkontakte"))

        extra: Dict[str, Any] = getattr(config, "extra", {}) or {}

        self.token: str = config.token or os.getenv("VK_TOKEN", "")
        self.api_version: str = os.getenv("VK_API_VERSION") or extra.get("api_version", "5.199")

        try:
            self.polling_timeout: int = int(
                os.getenv("VK_POLLING_TIMEOUT") or extra.get("polling_timeout", 25)
            )
        except (ValueError, TypeError):
            self.polling_timeout = 25

        self._allow_all: bool = (
            os.getenv("VK_ALLOW_ALL_USERS", "").lower() in {"1", "true", "yes"}
            or str(extra.get("allow_all", "")).lower() in {"1", "true", "yes"}
        )

        raw_allowed = os.getenv("VK_ALLOWED_USERS", "") or extra.get("allowed_users", "")
        if isinstance(raw_allowed, list):
            self._allowed_users: set = {str(u).strip() for u in raw_allowed if u}
        else:
            self._allowed_users = {u.strip() for u in str(raw_allowed).split(",") if u.strip()}

        self.home_channel: Optional[str] = (
            os.getenv("VK_HOME_CHANNEL") or str(extra.get("home_channel", "")) or None
        )

        # Runtime state — populated during connect()
        self._vk: Any = None
        self._vk_tools: Any = None
        self._longpoll: Any = None
        self.group_id: Optional[int] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_flag = threading.Event()
        self._poll_thread: Optional[threading.Thread] = None

    @property
    def name(self) -> str:
        return "VKontakte"

    # ── Authorization ────────────────────────────────────────────────────

    def _is_authorized(self, user_id: str) -> bool:
        if self._allow_all or not self._allowed_users:
            return True
        return user_id in self._allowed_users

    # ── Connection lifecycle ─────────────────────────────────────────────

    async def connect(self) -> bool:
        if not self.token:
            logger.error("VK: VK_TOKEN is not set")
            return False

        result = await asyncio.to_thread(self._init_session_sync)
        if result is None:
            return False

        self._vk, self._vk_tools, self._longpoll, self.group_id = result
        self._loop = asyncio.get_running_loop()
        self._stop_flag.clear()

        self._poll_thread = threading.Thread(
            target=self._poll_thread_fn, daemon=True, name="vk-longpoll"
        )
        self._poll_thread.start()
        logger.info("VK: connected as group id=%s", self.group_id)
        return True

    def _init_session_sync(self) -> Optional[tuple]:
        """Initialise VK API session synchronously (runs in thread)."""
        try:
            session = vk_api.VkApi(token=self.token, api_version=self.api_version)
            tools = session.get_api()
            groups = tools.groups.getById()
            if not groups:
                logger.error("VK: could not resolve group_id — check VK_TOKEN")
                return None
            group_id = int(groups[0]["id"])
            longpoll = VkBotLongPoll(session, group_id=group_id)
            return session, tools, longpoll, group_id
        except Exception as exc:
            logger.error("VK: session initialisation failed — %s", exc)
            return None

    async def disconnect(self) -> None:
        self._stop_flag.set()
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5.0)
        self._poll_thread = None
        self._vk = None
        self._longpoll = None
        logger.info("VK: disconnected")

    # ── Long Poll receive loop (background thread) ────────────────────────

    def _poll_thread_fn(self) -> None:
        """Blocking Long Poll loop — runs in a daemon thread."""
        backoff = 1.0
        while not self._stop_flag.is_set():
            try:
                for event in self._longpoll.listen():
                    if self._stop_flag.is_set():
                        return
                    try:
                        msg_event = self._vk_event_to_message_event(event)
                    except Exception as exc:
                        logger.warning("VK: event mapping error — %s", exc)
                        continue
                    if msg_event is not None:
                        asyncio.run_coroutine_threadsafe(
                            self.handle_message(msg_event), self._loop
                        )
                backoff = 1.0
            except Exception as exc:
                if self._stop_flag.is_set():
                    return
                logger.warning("VK: long poll error (retry in %.1fs) — %s", backoff, exc)
                self._stop_flag.wait(timeout=backoff)
                backoff = min(backoff * 2 + random.uniform(0, 1), 60.0)

    # ── Inbound message normalisation ────────────────────────────────────

    def _vk_event_to_message_event(self, event: Any) -> Optional[MessageEvent]:
        if event.type != VkBotEventType.MESSAGE_NEW:
            return None

        msg: Dict[str, Any] = event.obj.message
        peer_id = str(msg["peer_id"])
        from_id = str(msg["from_id"])
        text: str = msg.get("text") or ""

        # Skip bot's own outbound echoes (negative from_id = community)
        if int(from_id) < 0:
            return None

        if not self._is_authorized(from_id):
            logger.debug("VK: unauthorized user %s", from_id)
            return None

        is_group_chat = msg["peer_id"] >= _CHAT_PEER_OFFSET
        chat_type = "group" if is_group_chat else "dm"
        chat_name = f"chat_{peer_id}" if is_group_chat else from_id
        user_name = self._resolve_user_name_sync(from_id)

        file_paths: List[str] = []
        message_type = MessageType.TEXT

        for att in msg.get("attachments") or []:
            att_type = att.get("type", "")
            try:
                if att_type == "photo":
                    path = self._download_photo(att["photo"])
                    if path:
                        file_paths.append(path)
                        if message_type == MessageType.TEXT:
                            message_type = MessageType.PHOTO
                elif att_type == "audio_message":
                    path = self._download_audio_message(att["audio_message"])
                    if path:
                        file_paths.append(path)
                        message_type = MessageType.VOICE
                elif att_type == "doc":
                    path = self._download_doc(att["doc"])
                    if path:
                        file_paths.append(path)
            except Exception as exc:
                logger.warning("VK: failed to cache attachment type=%s — %s", att_type, exc)

        source = self.build_source(
            chat_id=peer_id,
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=from_id,
            user_name=user_name,
        )

        return MessageEvent(
            source=source,
            message_id=str(msg.get("conversation_message_id") or msg.get("id", "")),
            text=text,
            message_type=message_type,
            attachments=file_paths or None,
        )

    def _resolve_user_name_sync(self, user_id: str) -> str:
        try:
            users = self._vk_tools.users.get(user_ids=user_id, fields="")
            if users:
                u = users[0]
                return f"{u.get('first_name', '')} {u.get('last_name', '')}".strip()
        except Exception:
            pass
        return user_id

    # ── Attachment download helpers (sync, called from poll thread) ───────

    def _download_photo(self, photo: Dict[str, Any]) -> Optional[str]:
        sizes = photo.get("sizes") or []
        if not sizes:
            return None
        best = max(sizes, key=lambda s: s.get("width", 0) * s.get("height", 0))
        url = best.get("url", "")
        if not url:
            return None
        data = urllib.request.urlopen(url, timeout=15).read()
        return cache_image_from_bytes(data, ext=".jpg")

    def _download_audio_message(self, audio_msg: Dict[str, Any]) -> Optional[str]:
        url = audio_msg.get("link_ogg") or audio_msg.get("link_mp3") or ""
        if not url:
            return None
        ext = ".ogg" if "link_ogg" in audio_msg and audio_msg["link_ogg"] else ".mp3"
        data = urllib.request.urlopen(url, timeout=15).read()
        return cache_audio_from_bytes(data, ext=ext)

    def _download_doc(self, doc: Dict[str, Any]) -> Optional[str]:
        url = doc.get("url", "")
        title = doc.get("title", "file")
        if not url:
            return None
        data = urllib.request.urlopen(url, timeout=30).read()
        return cache_document_from_bytes(data, filename=title)

    # ── Outbound messages ────────────────────────────────────────────────

    async def send(self, chat_id: str, text: str, **kwargs: Any) -> SendResult:
        if not text:
            return SendResult(success=True)
        chunks = _split_text(text, MAX_MESSAGE_LENGTH)
        last: SendResult = SendResult(success=True)
        for chunk in chunks:
            last = await asyncio.to_thread(self._send_text_sync, chat_id, chunk)
            if not last.success:
                return last
        return last

    def _send_text_sync(self, chat_id: str, text: str) -> SendResult:
        try:
            msg_id = self._vk_tools.messages.send(
                peer_id=int(chat_id),
                message=text,
                random_id=random.randint(-2_000_000_000, 2_000_000_000),
                dont_parse_links=0,
            )
            return SendResult(success=True, message_id=str(msg_id))
        except Exception as exc:
            logger.error("VK: send failed to peer_id=%s — %s", chat_id, exc)
            return SendResult(success=False, error=str(exc))

    async def send_typing(self, chat_id: str, **kwargs: Any) -> None:
        try:
            await asyncio.to_thread(
                self._vk_tools.messages.setActivity,
                peer_id=int(chat_id),
                type="typing",
            )
        except Exception as exc:
            logger.debug("VK: typing indicator failed — %s", exc)

    async def send_image(
        self, chat_id: str, image_url: str, caption: str = "", **kwargs: Any
    ) -> SendResult:
        try:
            data = await asyncio.to_thread(
                lambda: urllib.request.urlopen(image_url, timeout=30).read()
            )
            return await asyncio.to_thread(
                self._upload_photo_sync, chat_id, data, caption
            )
        except Exception as exc:
            logger.error("VK: send_image failed — %s", exc)
            fallback = f"{caption}\n{image_url}".strip() if caption else image_url
            return await self.send(chat_id, fallback)

    def _upload_photo_sync(self, chat_id: str, data: bytes, caption: str) -> SendResult:
        upload = vk_api.VkUpload(self._vk)
        response = upload.photo_messages(photos=io.BytesIO(data))
        if not response:
            return SendResult(success=False, error="Photo upload returned empty response")
        photo = response[0]
        attachment = f"photo{photo['owner_id']}_{photo['id']}"
        try:
            msg_id = self._vk_tools.messages.send(
                peer_id=int(chat_id),
                message=caption or "",
                attachment=attachment,
                random_id=random.randint(-2_000_000_000, 2_000_000_000),
            )
            return SendResult(success=True, message_id=str(msg_id))
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def send_document(
        self, chat_id: str, file_path: str, caption: str = "", **kwargs: Any
    ) -> SendResult:
        try:
            return await asyncio.to_thread(
                self._upload_doc_sync, chat_id, file_path, caption
            )
        except Exception as exc:
            logger.error("VK: send_document failed — %s", exc)
            return SendResult(success=False, error=str(exc))

    def _upload_doc_sync(self, chat_id: str, file_path: str, caption: str) -> SendResult:
        upload = vk_api.VkUpload(self._vk)
        response = upload.document_message(
            doc=file_path,
            peer_id=int(chat_id),
            title=caption or os.path.basename(file_path),
        )
        if not response or "doc" not in response:
            return SendResult(success=False, error="Document upload failed")
        doc = response["doc"]
        attachment = f"doc{doc['owner_id']}_{doc['id']}"
        try:
            msg_id = self._vk_tools.messages.send(
                peer_id=int(chat_id),
                message="",
                attachment=attachment,
                random_id=random.randint(-2_000_000_000, 2_000_000_000),
            )
            return SendResult(success=True, message_id=str(msg_id))
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def send_voice(
        self, chat_id: str, audio_path: str, **kwargs: Any
    ) -> SendResult:
        try:
            return await asyncio.to_thread(
                self._upload_voice_sync, chat_id, audio_path
            )
        except Exception as exc:
            logger.error("VK: send_voice failed — %s", exc)
            return SendResult(success=False, error=str(exc))

    def _upload_voice_sync(self, chat_id: str, audio_path: str) -> SendResult:
        upload = vk_api.VkUpload(self._vk)
        response = upload.audio_message(audio=audio_path, peer_id=int(chat_id))
        if not response or "audio_message" not in response:
            return SendResult(success=False, error="Voice upload failed")
        am = response["audio_message"]
        attachment = f"audio_message{am['owner_id']}_{am['id']}"
        try:
            msg_id = self._vk_tools.messages.send(
                peer_id=int(chat_id),
                message="",
                attachment=attachment,
                random_id=random.randint(-2_000_000_000, 2_000_000_000),
            )
            return SendResult(success=True, message_id=str(msg_id))
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    # ── Chat info ────────────────────────────────────────────────────────

    async def get_chat_info(self, chat_id: str) -> dict:
        try:
            return await asyncio.to_thread(self._get_chat_info_sync, chat_id)
        except Exception as exc:
            logger.debug("VK: get_chat_info failed for %s — %s", chat_id, exc)
            return {"name": chat_id, "type": "dm", "chat_id": chat_id}

    def _get_chat_info_sync(self, chat_id: str) -> dict:
        peer_id = int(chat_id)
        if peer_id >= _CHAT_PEER_OFFSET:
            convs = self._vk_tools.messages.getConversationsById(peer_ids=peer_id)
            items = convs.get("items") or []
            if items:
                title = items[0].get("chat_settings", {}).get("title", chat_id)
                return {"name": title, "type": "group", "chat_id": chat_id}
            return {"name": chat_id, "type": "group", "chat_id": chat_id}

        users = self._vk_tools.users.get(user_ids=peer_id, fields="")
        if users:
            u = users[0]
            name = f"{u.get('first_name', '')} {u.get('last_name', '')}".strip()
            return {"name": name, "type": "dm", "chat_id": chat_id}
        return {"name": chat_id, "type": "dm", "chat_id": chat_id}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _split_text(text: str, limit: int) -> List[str]:
    """Split text into chunks not exceeding VK's message length limit."""
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        split_at = text.rfind("\n", 0, limit)
        if split_at <= 0:
            split_at = limit
        chunks.append(text[:split_at].rstrip())
        text = text[split_at:].lstrip()
    return chunks


# ---------------------------------------------------------------------------
# Plugin entry point
# ---------------------------------------------------------------------------


def register(ctx: Any) -> None:
    """Entry point called by the Hermes plugin system."""
    ctx.register_platform(
        name="vkontakte",
        adapter_cls=VKPlatformAdapter,
        check_fn=check_vkontakte_requirements,
    )
