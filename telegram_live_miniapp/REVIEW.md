# Code review: `LIVE ZOT` (V9.79)

Файл `app.py` — 7641 строка, 264 функции, всё в одном модуле. Прошёл по горячим путям: HMAC-проверка, фильтр, нотификации, треды/локи, HTTP-обработчик, IGScore-клиент.

---

## 🔴 КРИТИЧЕСКИЕ БАГИ ЛОГИКИ

### 1. `sanitize_notify_filter` молча выбрасывает 4–5 полей фильтра
Файл: `app.py:6421-6449` vs `app.py:6478-6536`.

`match_passes_filter` читает:
- `signal_type`, `goal_signal_enabled` (строка 6483)
- `goals_max` (6499)
- `score_diff_max` (6502)
- `pressure_min` (6519)

А `sanitize_notify_filter` возвращает только: `enabled, minute_min/max, shots_min, on_target_min, off_target_min, dangerous_min, attacks_min, corners_min, yellow_cards_min, red_cards_min, possession_min, scores, countries`.

**Что это значит на практике**: пользователь в Mini App двигает ползунок «макс. голов», «разница в счёте», «давление, %» — а на сервере при `subscribe` эти поля удаляются. В `notify_worker_loop` фильтр уже без них → условия НИКОГДА не срабатывают, дефолты `0/–1` дают «всегда true». То есть половина ползунков в UI бесполезна.

**Фикс**: добавить в возврат `sanitize_notify_filter`:
```python
"goals_max":      max(0, _safe_int(src.get("goals_max"), 0)),
"score_diff_max": _safe_int(src.get("score_diff_max"), -1),
"pressure_min":   max(0, min(100, _safe_int(src.get("pressure_min"), 0))),
"signal_type":    str(src.get("signal_type") or ""),
"goal_signal_enabled": bool(src.get("goal_signal_enabled", True)),
```

### 2. `minute_min > minute_max` не нормализуется
`app.py:6436-6437` — каждое поле клампится в `[0,130]` независимо. Если пользователь случайно поставил `min=65, max=45`, фильтр перестаёт пропускать что-либо, а в UI это выглядит как «бот сломался».

**Фикс**: после клампа сделать `if minute_min > minute_max: minute_min, minute_max = minute_max, minute_min`.

### 3. Отсутствует `goals_min`
Есть только `goals_max`. Невозможно сделать стратегию «жду 2-й тайм при счёте 1-1». Учитывая, что бот позиционируется как помощник для разбора live, это пробел в продукте.

### 4. Двойная (бесполезная) запись `goal_wait["enabled"]`
`app.py:5788-5792`:
```python
if goal_wait.get("enabled"):
    goal_wait["enabled"] = False
    changed = True
goal_wait["enabled"] = False        # ← всегда выполняется, ниже if
sub["goal_wait"] = goal_wait
```
Вторая строка делает условие выше бессмысленным для отметки `changed=True`: даже если пользователь отключал `goal_wait` сам, мы переписываем поле ещё раз и не отслеживаем это. Не баг — но запах.

### 5. `possession_min`: магия `or 50`
`app.py:6516`:
```python
poss = max(_stat_side_from_match_stats(s, "possession", "home") or 50,
           _stat_side_from_match_stats(s, "possession", "away") or 50)
```
Если статистика владения ещё не пришла (`0`), фолбэк = `50`, и фильтр считает что владение 50% — то есть **всегда проходит порог `possession_min ≤ 50`**. Нужно: либо `or 0`, либо явный «нет данных → не пропускать».

### 6. `_filter_signal_for_match` `pressure_min` дефолт `60`
`app.py:6526`:
```python
if max_pct < _safe_int(f.get("pressure_min"), 60):
```
Если `pressure_min` отсутствует в фильтре (а из-за бага №1 он ВСЕГДА отсутствует) — дефолт `60` всё равно применяется. То есть «давление ≥ 60%» работает молча, без воли пользователя.

---

## 🔴 БЕЗОПАСНОСТЬ

### 7. `parse_json_body` без верхней границы → DoS
`app.py:7266-7274`:
```python
length = int(handler.headers.get("Content-Length") or 0)
raw = handler.rfile.read(length)
```
Атакующий ставит `Content-Length: 10000000000` — сервер пытается прочесть 10 ГБ в RAM. Один такой запрос валит процесс. Нужно:
```python
MAX_BODY = 64 * 1024
if length > MAX_BODY: return {}
```

### 8. SSRF через `_download_team_logo`
URL логотипа берётся из ответа IGScore и скармливается `urllib.request.urlopen` без валидации схемы и хоста. Если IGScore (или MITM) отдаст `http://169.254.169.254/latest/meta-data/` или `file:///etc/passwd`, бот это скачает. Нужно: allowlist схем (`https://`), и опционально — резолв + проверка что не приватный диапазон.

### 9. `/api/match/avg` без rate-limit
`app.py:7474-7479`. Все соседние эндпоинты ограничены через `_rate_limit_ok`, а этот — нет. Опасно тем, что `avg_payload_for_match` под капотом может дёрнуть `team_recent` на IGScore.

### 10. Static-обработчик не резолвит путь
`app.py:7533-7539`:
```python
safe = Path(path.lstrip("/"))
if ".." in safe.parts: return ...
file_path = STATIC_DIR / safe
```
Защищает от `../`, но не от symlink-ов внутри `static/`. Лучше:
```python
file_path = (STATIC_DIR / safe).resolve()
if not file_path.is_relative_to(STATIC_DIR.resolve()):
    return text_response(self, "Forbidden", status=403)
```

### 11. `init_data` без `auth_date` проходит как «свежий»
`app.py:5759`:
```python
auth_age = time.time() - int(parsed.get("auth_date", "0"))
```
Если поля нет → `auth_age ≈ time.time() ≈ 1.7e9` → больше любого `MAX_AGE` → отклоняется. ОК.
Но если поле есть и равно `"0"` — тот же эффект. Безопасно. Я перепроверил — здесь ОК, оставляю как замечание для уверенности.

### 12. Бот-токен в логе
`app.py:7618`: `print(f"[notify] worker started with BOT_TOKEN (***{BOT_TOKEN[-4:]})")` — последние 4 символа в STDOUT. На Render это попадает в лог. Не критично, но желательно убрать.

### 13. Нет security-заголовков
В ответах нет `X-Content-Type-Options: nosniff`, `Referrer-Policy`, `X-Frame-Options`. Для встраивания в Telegram WebView некритично, но `nosniff` стоит добавить.

---

## 🟠 ПОТОКИ, ЛОКИ, МАСШТАБИРОВАНИЕ

### 14. `_notify_lock` удерживается на весь скан
`app.py:6810-6935` — `with _notify_lock:` оборачивает **полный** цикл «matches × subs», включая вызовы `_send_or_queue_telegram` и сохранение в БД. Пока крутится скан (десятки секунд при многих подписках), **все** входящие POST-запросы (`/api/subscribe`, `/api/notify`, `/api/notify/clear`, ...) стоят.

**Фикс**: внутри лока — только snapshot (`subs = list(_notify_subs.items())`), потом обработка снаружи, затем короткий лок для записи изменений.

### 15. Возможный deadlock между `_notify_lock` и `_admin_state_lock`
`_handle_permanent_telegram_failure` (`app.py:5779`) берёт сначала `_notify_lock`, потом `_admin_state_lock`. Любая обратная цепочка вызовов даст взаимоблокировку. Прямо сейчас я обратного порядка не нашёл, но `_notify_lock` — обычный `Lock`, не `RLock`, поэтому любой повторный заход из того же треда тоже подвесит сервер.

**Рекомендация**: документировать порядок захвата (`notify → admin → online`) и не вызывать `_handle_permanent_telegram_failure` из путей, где `_notify_lock` уже захвачен.

### 16. Thundering herd в `load_live_payload`
`app.py:2656-2716` — на cache miss блокировка `_cache_lock` берётся только на запись результата, сам `fetch_live_igscore()` идёт без лока. 100 параллельных HTTP-запросов от пользователей → 100 одновременных запросов к IGScore. Нужен механизм «single-flight» (один поток фетчит, остальные ждут результат).

### 17. Гонка в `_telegram_send_stats`
`app.py:5850, 5867, 5882` и т.д. — `dict[k] = _safe_int(dict.get(k), 0) + 1`. Это read-modify-write без лока, под нагрузкой инкременты теряются. Не критично (только статистика), но потом будет «непонятно, почему числа не сходятся».

**Фикс**: один лок на статы или `collections.Counter` + лок.

### 18. `_rate_limit_ok` cleanup чистит «первые», а не «старые»
`app.py:7359-7361`:
```python
for k in list(_api_rate_buckets.keys())[:1000]:
    _api_rate_buckets.pop(k, None)
```
Удаляет первые 1000 ключей по порядку вставки. Если эти 1000 — как раз активные пользователи, они снова станут «новыми» и реальная защита размывается.

**Фикс**: пройтись по словарю и удалить ключи с пустыми/устаревшими массивами; либо LRU.

### 19. `ThreadingHTTPServer` под нагрузкой
Стандартный `http.server` — это поток на запрос + GIL. Под 200+ rps это узкое горлышко. Для текущей нагрузки одного бота — норм, но при росте до серьёзного объёма стоит мигрировать на `aiohttp`, `uvicorn+fastapi` или хотя бы поставить `gunicorn -k gthread`.

### 20. Background HTTP-вызовы могут забить очередь
`COLLECTOR_WORKERS=2`, `COLLECTOR_INTERVAL=60s`, `LIVE_STATS_WORKERS=8`. Если IGScore начнёт тормозить (5-10 с/запрос), collector не уложится в `COLLECTOR_INTERVAL`, `last_finish` устареет, `/ready` отдаст 503. Понятно. Но никакого back-pressure механизма нет: при росте задержки очередь IGScore-запросов растёт линейно по числу live-матчей.

---

## 🟡 АРХИТЕКТУРА И ПОДДЕРЖИВАЕМОСТЬ

### 21. 7641 строка в одном файле
264 функции, локи разбросаны по всему модулю, глобальное состояние перемешано с эндпоинтами. Любая модификация — риск регрессии в несвязанной фиче. Очень больно ревьюить и тестировать.

**Минимальная декомпозиция**:
```
app/
  __init__.py
  config.py          # все os.environ.get(...)
  igscore_client.py  # post_json, match_info, match_stats, ...
  live_cache.py      # _init_live_cache_db, collector_loop
  filters.py         # sanitize/match_passes_filter/_filter_signal_for_match
  notify/
    worker.py        # notify_worker_loop, _scan_notify_subscription
    storage.py       # _load/_save_notify_subs, _notify_table
    telegram.py      # send_telegram_message, queue
  admin/
    panel.py
    state.py
  http_handler.py    # MiniAppHandler
  main.py
```

### 22. IGScore без официального API
Бот ходит на `api.igscore.net:8080` с подделанными `User-Agent`, `Origin`, `Referer`. Любое изменение фронта IGScore → бот ломается, и вы об этом узнаёте от пользователей. Стоит:
- мониторить error rate `/v1/football/competition/list` → алерт в Telegram админу;
- иметь fallback на 2-го провайдера (SofaScore, FlashScore, API-Football);
- кешировать упавшие ответы дольше (грейс), а не отдавать DEMO.

### 23. Хранилище подписок: JSON / SQLite / Postgres
Три ветки в каждом `_load_*/_save_*`. Половина миграционных флагов (`NOTIFY_MIGRATE_JSON`, `NOTIFY_MIGRATE_SQLITE`). Это нормально для эволюции, но пора зафиксировать Postgres как единственный production-режим и удалить JSON/SQLite-ветки (или вынести в отдельный `compat.py`).

### 24. `_save_notify_subs` пишет **весь словарь** на каждое изменение
`app.py:5710` → `_save_notify_table("notify_subs", _notify_subs)`. Под `_notify_lock`. При 10k пользователей это 10к JSON-сериализаций при каждой смене статуса одного из них.

**Фикс**: переходить на per-row upsert. Это самая большая зона для оптимизации после уровня логики.

### 25. Конфиг через 80+ переменных окружения
Это управляемо, но я бы:
- сгруппировал в датаклассы (`NotifyConfig`, `CollectorConfig`, `TelegramConfig`);
- логировал значения при старте (без секретов) — чтобы можно было быстро понять, в каком режиме запущен инстанс.

---

## 🟢 ПРОДУКТ / UX

### 26. Дефолты фильтра очень узкие
`sanitize_notify_filter` по умолчанию: `attacks_min=101, dangerous_min=51, shots_min=14, on_target_min=6, minute_min=45, minute_max=65`. Это «сильный» сигнал, но без подсказки в UI новый пользователь подумает «бот вообще ничего не присылает». Стоит:
- Иметь 3 пресета: «мягкий / средний / строгий» (у вас уже есть `profile_id`, добейте до конца).
- На subscribe возвращать ожидание «совпало X / Y live матчей сейчас» — пользователь сразу понимает, насколько узок фильтр.

### 27. Нет «глушилки» на одну подписку
Если у пользователя матч идёт и за 2 минуты статистика трижды пересекает порог, он получит несколько уведомлений (защита есть через `delivery_key + score`, но при изменении счёта ключ другой). Стоит явный `NOTIFY_COOLDOWN_PER_MATCH` — он у вас есть в env, проверить, что реально применяется в notify_worker.

### 28. Ссылка `notification_match_link` без `BOT_USERNAME` молча отдаёт `""`
`app.py:5727-5739` — если `BOT_USERNAME` не задан и `PUBLIC_BASE_URL` пуст, вернётся пустая строка, и кнопка «Открыть» в Telegram-сообщении не появится. Стоит логировать предупреждение при старте.

### 29. `_filter_rating_label` использует эмодзи, но не локализуется
Жёстко зашитые русские строки — норм, бот русскоязычный. Просто отметить.

---

## ✅ ЧТО СДЕЛАНО ХОРОШО

- HMAC-проверка `initData` (`verify_init_data`) — каноничная, с `compare_digest`.
- Дедуп уведомлений: `delivered/dismissed/seen` бакеты + legacy keys — хорошо.
- Постоянная очередь Telegram-сообщений с ретраями и backoff'ом + persistent jobs (`_persist_telegram_job`).
- Авто-отключение подписки при HTTP 400/403 от Telegram.
- Кэши с TTL на match_info / pressure_chart / team_avg — снимают нагрузку с IGScore.
- WAL + `synchronous=NORMAL` на SQLite — правильный выбор.
- `STALE_LIVE_HIDE_AFTER_MINUTES` и `_cleanup_finished_matches` — отдельная похвала за гигиену БД.

---

## 📋 ПОРЯДОК ДЕЙСТВИЙ

Если делать фиксы по приоритету:

1. **Сегодня** (баги, ломающие продукт):
   - § 1 `sanitize_notify_filter` — добавить пропавшие поля.
   - § 2 — нормализовать min/max минуты.
   - § 5 — убрать `or 50` в `possession_min`.
   - § 7 — лимит на `Content-Length` в `parse_json_body`.

2. **На неделе** (стабильность):
   - § 14 — освободить `_notify_lock` на время скана.
   - § 16 — single-flight для `load_live_payload`.
   - § 8 — валидация URL в `_download_team_logo`.
   - § 9 — rate-limit на `/api/match/avg`.

3. **На месяц** (масштаб и поддерживаемость):
   - § 21 — декомпозиция на пакет.
   - § 24 — per-row upsert вместо записи всего словаря.
   - § 26 — пресеты фильтра.
   - § 22 — fallback-провайдер live-данных.
