# Быстрый деплой на Render

Важно: файлы из этого архива должны лежать в корне GitHub репозитория, рядом с `app.py`, `requirements.txt`, `render.yaml` и `Dockerfile`.

## Вариант 1 — Python Web Service

Build Command:

```bash
pip install -r requirements.txt
```

Start Command:

```bash
python app.py
```

Environment:

```text
HOST=0.0.0.0
COLLECTOR_ENABLED=1
LIVE_FROM_DB_ONLY=1
```

`PORT` руками ставить не обязательно. Render сам дает порт, а приложение читает его из переменной `PORT`.

## Вариант 2 — Docker

Если выбираешь Docker Environment, оставь Dockerfile из архива в корне репозитория. Start Command можно оставить пустым, используется CMD из Dockerfile.

## Частая ошибка

Если загрузить папку целиком так, что получится `telegram_live_miniapp/app.py`, а в корне репозитория нет `app.py`, Render не найдет проект. Поэтому загружай именно содержимое архива в корень репозитория.
