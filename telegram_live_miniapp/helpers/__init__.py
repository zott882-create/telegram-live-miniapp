"""v9.76: чистые helper-модули без зависимостей от глобального состояния app.py.

Это первый шаг разбиения 6900-строчного app.py. Здесь живут функции,
которые НЕ работают с _notify_subs / _live_cache / БД — они принимают
аргументы и возвращают результат.

Дальнейшие модули по плану:
    - storage.py   (SQLite/Postgres абстракция для notify)
    - igscore.py   (HTTP-клиент к IGScore)
    - notifier.py  (worker, send_telegram, отправка)
    - admin.py     (admin panel & callbacks)
    - server.py    (HTTPHandler)
"""
