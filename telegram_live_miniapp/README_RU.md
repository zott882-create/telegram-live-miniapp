# Telegram Live Mini App — v5 PREMIUM

Премиум Mini App для Telegram с лайв-матчами в стиле igscore.net.

## Что нового в v5

1. **Премиум дизайн чёрно-зелёный** — главная как igscore.net, не "в разнабой"
2. **Матчи группируются по лигам** — TOP-страны (Англия, Испания, Италия...) и TOP-лиги выводятся первыми
3. **Фильтры на всю статистику** — удары, в створ, опасные, атаки, угловые, владение, счёт, минуты
4. **Карточка матча с 3 табами** — Лайв-стат, Все цифры, Среднее за 10 матчей
5. **Вкладка избранных** — внизу в навигации
6. **Уведомления** — отдельная вкладка с фильтром, пуши в Telegram и in-app
7. **Логотипы команд** — сервер ищет logo-поля в IGScore API, сохраняет URL/локальную копию в SQLite и показывает логотипы в списке и карточке матча

## Структура

```
telegram_live_miniapp/
├── app.py              ← Python-сервер (stdlib http.server, без зависимостей)
├── static/
│   ├── index.html      ← Mini App (HTML + CSS + JS в одном файле)
│   └── styles.css      ← премиум-стили v5
├── data/               ← (создаётся при запуске) notify_subs.json, team_logos.sqlite3, team_logos/
├── Procfile            ← `web: python app.py` для Render
├── Dockerfile          ← опционально, для Docker-деплоя
├── requirements.txt    ← (пусто — нет внешних зависимостей)
└── set_telegram_menu_button.py  ← скрипт для @BotFather
```

## Запуск локально

```bash
cd telegram_live_miniapp
python3 app.py
```

Открыть: http://127.0.0.1:8080

Можно сразу попробовать demo-режим (3 матча с лайв-статистикой):
```bash
DATA_MODE=demo python3 app.py
```

## Деплой на Render

1. Загрузи папку `telegram_live_miniapp` на GitHub
2. Render → New → Web Service → выбрать репозиторий
3. **Root Directory**: `telegram_live_miniapp`
4. **Start command**: `python app.py` (Render найдёт сам через Procfile)
5. Environment Variables:

| Переменная | Значение | Зачем |
|---|---|---|
| `DATA_MODE` | `auto` | auto / igscore / sqlite / demo |
| `BOT_TOKEN` | `<токен @BotFather>` | для пушей в Telegram |
| `LIVE_DB_PATH` | `/path/to/live_history.sqlite3` | если используешь sqlite-режим |
| `NOTIFY_POLL_INTERVAL` | `30` | частота проверки (сек) |
| `NOTIFY_COOLDOWN_PER_MATCH` | `600` | пауза между пушами по одному матчу |
| `TEAM_LOGO_DOWNLOAD` | `1` | скачивать найденные логотипы локально в `data/team_logos/`; `0` = хранить только URL |
| `TEAM_LOGO_MAX_BYTES` | `1500000` | максимальный размер одного файла логотипа |

6. После деплоя → @BotFather → /mybots → бот → Bot Settings → Menu Button → ввести URL приложения

## Endpoints

| Метод | Путь | Что делает |
|---|---|---|
| GET | `/healthz` | пинг |
| GET | `/api/live` | список лайв-матчей с группировкой по странам/лигам |
| GET | `/api/match?id=...` | детали матча + 10-матчевая статистика |
| POST | `/api/subscribe` | подписка на уведомления (init_data + filter) |
| POST | `/api/notify` | пуш Telegram-сообщения из открытого приложения |
| GET | `/team-logos/<file>` | отдаёт локально сохранённые логотипы команд |

## Как работают логотипы команд

1. При загрузке лайв-матчей сервер проверяет `homeTeam` / `awayTeam` и прямые поля матча (`homeLogo`, `homeLogoUrl`, `homeTeamLogo`, `awayLogo` и похожие).
2. Если логотип найден, он записывается в `data/team_logos.sqlite3`.
3. По умолчанию файл также скачивается в `data/team_logos/`, а frontend получает локальный путь вида `/team-logos/<file>`.
4. Если скачать не получилось, приложение всё равно сохранит внешний URL.
5. Если у команды нет логотипа, frontend показывает инициалы команды, чтобы карточка не ломалась.

## Как работают уведомления

1. Юзер открывает Mini App из бота → JS читает `tg.initData` (подписан ботом)
2. Юзер настраивает фильтр (минуты, удары и т.д.) и включает Toggle
3. Frontend POST-ит `/api/subscribe` с init_data + фильтром
4. Сервер верифицирует HMAC-подпись init_data → сохраняет `{chat_id, filter}` в `data/notify_subs.json`
5. Фоновый поток каждые 30 сек тянет `/api/live` и проверяет все подписки
6. Когда матч проходит фильтр — `sendMessage` в Telegram с кнопкой "↗ IGScore"
7. Cooldown 600s на один матч защищает от спама
8. Параллельно фронт сам добавляет алерт в in-app историю (без задержки)

**Без `BOT_TOKEN`** push в Telegram отключается, но in-app алерты в "Уведомления" работают.

## Совместимость с v4

- Endpoints `/api/live` и `/api/match` остались как были (бэкенд обратно-совместим)
- Структура полей матча та же — добавились опциональные `stats: {...}`, `home_logo`, `away_logo`
- localStorage-ключи переименованы (`miniapp-v5-*`) — старые избранные не перейдут, это сознательно (новый формат)
