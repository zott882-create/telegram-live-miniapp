# V10.5 hotfix

- LIVE code restored from V10.3, with a cold-start fallback to direct IGScore while the collector database is warming up.
- Prematch requests no longer block the browser while SportScore pages are loading.
- A background worker refreshes prematches every 5 minutes and on manual retry.
- The frontend no longer aborts `/api/prematch` after 20 seconds.
- Previous prematch data remains visible during temporary provider errors.
- Live, finished, cancelled and already-started fixtures are removed on provider, API and frontend layers.
- SportScore prematch IDs are never sent to the IGScore live-odds endpoint.
- Dated SportScore pages are fetched concurrently with an 8-second per-request timeout.
