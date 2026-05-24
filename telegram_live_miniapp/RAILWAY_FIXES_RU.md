# Railway fixed build

Что изменено в этом архиве:

1. Исправлен падёж collector: `match_events.created_at` заменён на существующую колонку `detected_at`.
2. Убран зашитый админ ID. Теперь админы задаются через Railway Variables: `ADMIN_IDS=твой_telegram_id`.
3. Demo/fallback режим больше не дёргает внешний odds API.
4. Первый demo-матч сделан подходящим под стандартный фильтр, чтобы можно было проверить Telegram-уведомления сразу.
5. В `requirements.txt` добавлен `requests` для `set_telegram_menu_button.py`.

Минимальные Railway Variables:

```env
HOST=0.0.0.0
BOT_TOKEN=токен_от_BotFather
BOT_USERNAME=имя_бота_без_@
ADMIN_IDS=твой_telegram_id
ADMIN_PANEL_ENABLED=1
ADMIN_POLLING_ENABLED=1
DATA_MODE=auto
```

Для теста уведомлений можно временно поставить:

```env
DATA_MODE=demo
```

Порядок проверки:

1. Напиши боту `/start`.
2. Открой Mini App именно из Telegram.
3. Включи уведомления.
4. Смотри Railway logs: должно быть `queue=1`, потом `OK=1`.

Если Railway показывает `subs=1 matches=3 queue=0`, значит фильтр слишком жёсткий или все совпадения уже были отправлены/подавлены. Для теста очисти уведомления в приложении или поставь мягкий фильтр.
