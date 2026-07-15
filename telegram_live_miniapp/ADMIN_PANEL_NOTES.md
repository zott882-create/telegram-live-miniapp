# Telegram admin panel

Owner Telegram ID is built into this build: `851766591`.

After deployment, open the bot chat and send `/admin`.

Available sections:
- statistics: known users, online users, live matches, matches found, last/average/max collection time;
- users: active filters, blocks and subscription status;
- subscriptions: grant 1/7/30/90/365 days, lifetime, custom number of days, check or revoke;
- access mode: `Доступ по подписке ON/OFF`;
- notification queue, system status and online user limit.

The subscription access requirement starts OFF for a safe update. First grant subscriptions, then open `Подписки` and switch `Доступ по подписке` to ON. When ON, users without an active subscription cannot enter the Mini App or enable Telegram notifications.

User commands:
- `/subscription` — subscription status;
- `/id` — Telegram ID.

Admin commands:
- `/admin`, `/stats`, `/users`, `/subs`, `/system`.

Required deployment variable: `BOT_TOKEN`. Existing variables such as `BOT_USERNAME`, `DATABASE_URL`, `REDIS_URL` remain unchanged.
