# Stability patch

This package keeps the original UI and adds a server-side stability layer.

Recommended Render environment variables:

```env
REDIS_URL=<your Render Redis internal URL>
DATABASE_URL=<your Render Postgres URL>
DATA_MODE=auto
COLLECTOR_ENABLED=1
LIVE_FROM_DB_ONLY=1
REDIS_CACHE_ENABLED=1
REDIS_RATE_LIMIT_ENABLED=1
REDIS_COLLECTOR_LOCK_ENABLED=1
LAST_GOOD_LIVE_TTL_SECONDS=1800
LAST_GOOD_MATCH_TTL_SECONDS=7200
IGSCORE_HTTP_RETRIES=2
IGSCORE_CIRCUIT_BREAKER_ENABLED=1
IGSCORE_CIRCUIT_FAILURE_THRESHOLD=5
IGSCORE_CIRCUIT_COOLDOWN_SECONDS=60
```

New endpoints:

- `/healthz` or `/livez` — app process is alive.
- `/readyz` or `/ready` — storage/collector readiness check.

Redis is optional. If Redis is down, the app falls back to memory/files instead of failing the deployment.
