# Установка VK-адаптера для Hermes Agent

## Быстрый старт

### 1. Клонировать и установить плагин

```bash
git clone https://github.com/h0tnanny/hermes-vk-connector ~/.hermes/plugins/vkontakte
```

### 2. Установить зависимость

```bash
# Через venv Hermes (рекомендуется)
/usr/local/lib/hermes-agent/venv/bin/pip install vk_api

# Или глобально
pip install vk_api
```

### 3. Настроить токен

Получите токен в настройках вашего VK-сообщества:
**Управление → Работа с API → Создать ключ**

Необходимые разрешения: **Сообщения сообщества**, **Фотографии**, **Документы**

Также включите Long Poll: **Управление → Сообщения → Настройки Long Poll API**,
отметьте событие **Входящее сообщение**.

```bash
export VK_TOKEN="vk1.a.ВАШ_ТОКЕН"
```

### 4. Запустить Gateway

```bash
hermes gateway
```

Проверить подключение:

```bash
hermes gateway status
```

VKontakte должен отображаться в списке активных платформ.

### 5. Написать боту

Напишите личное сообщение вашему VK-сообществу — агент ответит и покажет
постоянную клавиатуру с кнопками **🆕 Новый чат** и **🔄 Сброс**.

---

## Запуск как systemd-сервис

```bash
systemctl --user restart hermes-gateway
systemctl --user status hermes-gateway
```

---

## Рекомендуемый system prompt

Чтобы агент не использовал Markdown (VK его не рендерит):

```bash
hermes config set gateway.platforms.vkontakte.prompt_extra \
  "Ты отвечаешь пользователю ВКонтакте. Пиши простым текстом без Markdown (без **, _, #, \`). Вместо маркированных списков используй • или тире. Ссылки оставляй как есть. Эмодзи можно использовать."
```

---

## Переменные окружения

| Переменная           | Обязательная | По умолчанию | Описание |
|----------------------|:---:|---|---|
| `VK_TOKEN`           | ✅  | —       | Токен API сообщества |
| `VK_ALLOWED_USERS`   | ❌  | (все)   | ID пользователей через запятую |
| `VK_HOME_CHANNEL`    | ❌  | —       | peer_id для крон-уведомлений |
| `VK_API_VERSION`     | ❌  | `5.199` | Версия VK API |
| `VK_ALLOW_ALL_USERS` | ❌  | `false` | Разрешить всех (только для тестов) |
| `VK_POLLING_TIMEOUT` | ❌  | `25`    | Таймаут Long Poll (секунды) |

---

## Обновление

```bash
cd ~/.hermes/plugins/vkontakte
git pull
systemctl --user restart hermes-gateway
```
