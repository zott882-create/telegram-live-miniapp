# Деплой на Render / похожий сервис

В этом архиве `Dockerfile`, `app.py`, `requirements.txt`, `Procfile` и папка `static/` лежат прямо в корне.

## Вариант 1 — Root Directory пустой
В настройках сервиса оставь `Root Directory` пустым.

Build/Runtime: Docker или Dockerfile.
Dockerfile Path: `Dockerfile` или `./Dockerfile`.

## Вариант 2 — Root Directory = telegram_live_miniapp
На всякий случай внутри архива также есть папка `telegram_live_miniapp/` с полной копией проекта. Если в настройках сервиса стоит `Root Directory = telegram_live_miniapp`, деплой тоже найдёт `Dockerfile`.

## Обязательные переменные
BOT_TOKEN=твой_токен_бота
HOST=0.0.0.0
PORT=8080
COLLECTOR_ENABLED=1
LIVE_FROM_DB_ONLY=1

## Важно
Если деплоишь через GitHub, нужно распаковать ZIP и загрузить в репозиторий именно файлы проекта, а не сам ZIP-файл.
