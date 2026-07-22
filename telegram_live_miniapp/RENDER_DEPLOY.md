# Deploy to Render

## Important
Render does not deploy a ZIP directly. Extract this folder, upload its contents to a GitHub/GitLab/Bitbucket repository, and connect that repository to Render.

## Option A — Blueprint (recommended)
1. Extract the archive.
2. Create a repository and upload all files from this folder so `render.yaml`, `Dockerfile`, and `app.py` are in the repository root.
3. In Render: New -> Blueprint.
4. Connect the repository and apply `render.yaml`.
5. When prompted, enter:
   - `BOT_TOKEN` — token from BotFather.
   - `BOT_USERNAME` — username without `@`.
   - `ADMIN_IDS` — your numeric Telegram user ID; multiple IDs separated by commas.
6. Wait for the first deployment.
7. Open the assigned `https://<service>.onrender.com` URL and verify `/health`.
8. In Render -> Environment, add:
   - `PUBLIC_BASE_URL=https://<service>.onrender.com`
   - `MINIAPP_SHORT_NAME=<short name from BotFather>` when a named Mini App is used.
9. Save and redeploy.

## Option B — Web Service manually
- Runtime: Docker
- Health check path: `/health`
- Dockerfile: `./Dockerfile`
- No custom start command is required; Docker runs `python app.py`.

## Storage warning
Render Free has an ephemeral filesystem. Without `DATABASE_URL`, SQLite subscriptions, admin state, and cached data can disappear after restart, redeploy, or spin-down. For persistent state, connect Render Postgres and set `DATABASE_URL`; leave `NOTIFY_STORAGE=auto`.

## Free-plan warning
A Free web service can spin down when idle. This can delay opening the Mini App and stop background collection while the service is asleep. A continuously running paid instance is recommended for a production Telegram bot.

## Safe provider mode
Use initially:
- `LIVE_PROVIDER=igscore`
- `PREMATCH_ENABLED=1`

Switching LIVE to SportScore should happen only after its `/live/` JSON has been tested during real matches.
