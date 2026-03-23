# WB Chat Bot — Wildberries Buyers Chat Auto-Responder

Сервис автоматически отправляет сообщение в новые чаты покупателей на Wildberries через официальный Buyers Chat API.

> **Polling model**: сервис работает по модели polling (периодический опрос API). WB Buyers Chat API не поддерживает webhooks для chat events на момент создания этого проекта.

## WB Buyers Chat API — используемые endpoints

| Метод | Endpoint | Назначение |
|-------|----------|-----------|
| GET   | `/api/v1/seller/events` | Получение потока событий чатов |
| GET   | `/api/v1/seller/chats`  | Список чатов (fallback для replySign) |
| POST  | `/api/v1/seller/message` | Отправка сообщения (multipart/form-data) |

### Формат ответов WB

Все ответы WB обёрнуты в envelope:

```json
{
  "result": { ... },
  "errors": [...]
}
```

**Events** (`GET /api/v1/seller/events`):
```json
{
  "result": {
    "next": 1700000005000,
    "totalEvents": 2,
    "events": [
      { "chatID": "...", "eventID": "...", "isNewChat": true, "replySign": "..." }
    ]
  }
}
```

**Chats** (`GET /api/v1/seller/chats`):
```json
{
  "result": [
    { "chatID": "...", "replySign": "...", ... }
  ]
}
```

**Send message** (`POST /api/v1/seller/message`): multipart/form-data с полями `chatID`, `replySign`, `message`.

### Имена полей WB

Официальные имена: `chatID`, `eventID`, `replySign`, `isNewChat`. Сервис также поддерживает camelCase-варианты (`chatId`, `eventId`) как fallback.

### Cursor

Параметр `next` — целочисленный timestamp в миллисекундах. Сервис хранит его как integer в SQLite.

## Бизнес-логика

1. Сервис polling'ом опрашивает WB Buyers Chat API на наличие новых событий
2. При обнаружении нового чата (`isNewChat = true`):
   - Проверяет дедупликацию (SQLite)
   - Ждёт 5 секунд (настраивается)
   - В режиме `production`: отправляет сообщение "тест"
   - В режиме `dry-run`: логирует событие и уведомляет в Telegram
3. Все обработанные чаты сохраняются в SQLite для защиты от дублей
4. Cursor сохраняется между перезапусками

### Логика первого запуска

При первом запуске (cursor отсутствует в БД):
1. Сервис делает один запрос к `GET /api/v1/seller/events` без параметра `next`
2. Если WB вернул `next` — сохраняет его как стартовую позицию
3. Если WB не вернул `next` (нет событий) — использует текущий timestamp в мс
4. Если запрос завершился ошибкой — также использует текущий timestamp в мс

Таким образом, при первом запуске старые чаты не обрабатываются. Обрабатываются только чаты, появившиеся после старта сервиса.

### Фильтрация по товарам и оценкам

WB создаёт чат в ответ на отзыв покупателя. Сервис позволяет фильтровать, на какие чаты отвечать:

**По оценкам (рейтингу отзыва):**
```
REPLY_TO_RATINGS=1,2,3
```
Если указано — отвечать только на чаты с этими оценками. Пусто = отвечать на все.

**По товарам (nmID):**
```
PRODUCT_WHITELIST=12345678,87654321
PRODUCT_BLACKLIST=99999999
```
- **Whitelist** — если указан, отвечать ТОЛЬКО на эти товары
- **Blacklist** — если указан, НЕ отвечать на эти товары
- Оба пустые = отвечать на все товары
- Blacklist имеет приоритет над whitelist

**Логика при отсутствии данных в событии:**
Если фильтр настроен, но в событии нет нужного поля (rating / nmID), чат пропускается. Это безопасное поведение по умолчанию.

### Допущение v1

Если WB API не предоставляет явного поля, подтверждающего, что WB уже отправил автоматическое сообщение, используется логика: **новый чат → задержка 5 сек → отправка сообщения 1 раз**. Это может быть уточнено в следующих версиях.

## Установка на Mac

```bash
cd wb-chat-bot

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

## Настройка

```bash
cp .env.example .env

# Заполнить обязательные поля:
# - WB_API_TOKEN — токен из личного кабинета WB (раздел API, права на Чат с покупателями)
# - TELEGRAM_BOT_TOKEN — токен от @BotFather
# - TELEGRAM_CHAT_ID — ваш chat ID (получить через @userinfobot)
# - APP_MODE — dry-run или production
```

## Запуск

### Dry-run (безопасный режим, сообщения НЕ отправляются)

```bash
APP_MODE=dry-run python -m app.main
```

### Production (сообщения отправляются в WB)

```bash
APP_MODE=production python -m app.main
```

## Telegram-команды

| Команда   | Описание                              |
|-----------|---------------------------------------|
| /status   | Текущий статус сервиса (режим, uptime, cursor) |
| /pause    | Поставить сервис на паузу             |
| /resume   | Снять с паузы                         |
| /stats    | Статистика обработанных чатов         |
| /last     | Последние 5 обработанных чатов        |

## Системные уведомления в Telegram

- Сервис запущен / остановлен
- WB токен невалиден
- Ошибка WB API
- Успешная отправка сообщения
- Dry-run событие
- Rate limit (429)
- Пауза / снятие с паузы

## Проверка работоспособности

1. Запустить в dry-run — должен появиться статус в Telegram
2. Отправить /status в Telegram — должен ответить
3. Проверить файл `logs/app.log` — должны быть записи
4. Проверить `data/app.db` — должна быть структура таблиц

## Запуск тестов

```bash
python -m pytest tests/ -v
```

## Структура проекта

```
wb-chat-bot/
├── app/
│   ├── __init__.py
│   ├── main.py          # Точка входа, graceful shutdown
│   ├── config.py         # Загрузка .env, валидация
│   ├── wb_client.py      # HTTP-клиент к WB Buyers Chat API
│   ├── telegram_client.py # Telegram-бот: уведомления + команды
│   ├── storage.py        # SQLite: дедупликация + cursor
│   ├── service.py        # Основной polling loop + бизнес-логика
│   ├── commands.py       # Обработчики Telegram-команд
│   └── logger.py         # Настройка логирования
├── data/                 # SQLite БД (создаётся автоматически)
├── logs/                 # Лог-файлы (ротация)
├── tests/
│   └── test_service.py   # Unit-тесты
├── .env.example          # Шаблон конфигурации
├── requirements.txt      # Зависимости Python
└── README.md
```

## Перенос на сервер (VPS / Docker)

### VPS

```bash
git clone <repo-url> && cd wb-chat-bot
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Заполнить .env
```

Пример systemd unit:

```ini
[Unit]
Description=WB Chat Bot
After=network.target

[Service]
Type=simple
User=deploy
WorkingDirectory=/opt/wb-chat-bot
ExecStart=/opt/wb-chat-bot/venv/bin/python -m app.main
Restart=always
RestartSec=10
EnvironmentFile=/opt/wb-chat-bot/.env

[Install]
WantedBy=multi-user.target
```

### Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "-m", "app.main"]
```

```bash
docker build -t wb-chat-bot .
docker run -d --name wb-chat-bot --env-file .env -v ./data:/app/data -v ./logs:/app/logs wb-chat-bot
```

## Технические детали

- **Retry**: exponential backoff (2s, 4s, 8s) для 5xx и сетевых ошибок
- **Rate limit**: автоматическая пауза на Retry-After при 429
- **Дедупликация**: SQLite с UNIQUE constraint на chat_id
- **Cursor**: integer timestamp в ms, сохраняется в SQLite, восстанавливается после рестарта
- **Первый запуск**: cursor инициализируется из WB или текущим timestamp, старые чаты не обрабатываются
- **Отправка**: multipart/form-data через POST /api/v1/seller/message
- **Graceful shutdown**: SIGINT/SIGTERM корректно завершают сервис
- **Polling model**: сервис опрашивает WB API с настраиваемым интервалом (по умолчанию 3 сек)
