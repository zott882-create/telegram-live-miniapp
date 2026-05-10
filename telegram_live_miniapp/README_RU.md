# Telegram Live Matches Mini App

Готовая заготовка мини-приложения под Telegram по оформлению из скриншотов: главный экран live-матчей и карточка матча со статистикой.

## Что внутри

- `app.py` — Python-сервер без обязательных внешних библиотек.
- `static/index.html` — входная страница Mini App.
- `static/app.js` — логика интерфейса, фильтры, избранное, карточка матча.
- `static/styles.css` — тёмное оформление под Telegram.
- `set_telegram_menu_button.py` — помощник для подключения кнопки Mini App к боту, когда будет домен.
- `data/` — сюда можно положить `live_history_v1.sqlite3` от старого бота, если хочешь читать данные из базы.

## Быстрый запуск на ПК

```bash
python app.py
```

Открой в браузере:

```text
http://127.0.0.1:8080
```

По умолчанию режим `DATA_MODE=auto`:
1. пробует получить live-матчи с IGScore API;
2. если не получилось — пробует SQLite базу старого бота;
3. если базы нет — показывает demo-матчи, чтобы интерфейс всё равно запускался.

## Запуск от базы старого бота

Если старый бот уже собирает live-матчи в SQLite, скопируй файл:

```text
live_history_v1.sqlite3
```

в папку:

```text
data/live_history_v1.sqlite3
```

и запусти:

```bash
DATA_MODE=sqlite python app.py
```

На Windows PowerShell:

```powershell
$env:DATA_MODE="sqlite"
python app.py
```

Можно указать путь явно:

```bash
LIVE_DB_PATH=/path/to/live_history_v1.sqlite3 DATA_MODE=sqlite python app.py
```

## Запуск только через IGScore API

```bash
DATA_MODE=igscore python app.py
```

## Локальный тест в Telegram

Для Telegram Mini App нужен публичный HTTPS URL. Локально можно использовать туннель:

```bash
ngrok http 8080
```

или Cloudflare Tunnel:

```bash
cloudflared tunnel --url http://localhost:8080
```

После этого вставляешь HTTPS-ссылку в настройки Mini App.

## Подключение к Telegram-боту

Когда будет домен, например:

```text
https://example.com
```

можно задать кнопку меню:

```bash
BOT_TOKEN=123456:ABC WEBAPP_URL=https://example.com python set_telegram_menu_button.py
```

На Windows PowerShell:

```powershell
$env:BOT_TOKEN="123456:ABC"
$env:WEBAPP_URL="https://example.com"
python set_telegram_menu_button.py
```

## Для хостинга

Сервер слушает порт из переменной `PORT`, поэтому подходит для Render/Railway/VPS:

```bash
PORT=10000 HOST=0.0.0.0 python app.py
```

Для VPS можно поставить за Nginx и выдать HTTPS через Let's Encrypt.

## Важно

В этом архиве нет токенов Telegram, DeepSeek/OpenAI ключей и других секретов. Их нужно задавать только через переменные окружения на своём сервере.
