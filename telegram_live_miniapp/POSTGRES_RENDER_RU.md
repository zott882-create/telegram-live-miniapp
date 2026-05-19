# PostgreSQL для Telegram Live Mini App

Эта версия умеет хранить состояние пользователей в PostgreSQL.

## Что переносится в PostgreSQL

- настройки уведомлений пользователей;
- 3 профиля фильтров;
- включение/выключение `Ждём гол`;
- найденные матчи во вкладке уведомлений;
- cooldown/seen-состояния, чтобы не слать одно и то же уведомление постоянно;
- подписки на голы.

Live-кэш матчей и кэш логотипов могут оставаться локальными. Это технический кэш, а не пользовательские данные.

## Настройки Render

1. Создай Render Postgres.
2. Подключи его к web service или добавь переменную окружения:

```text
DATABASE_URL=postgresql://...
```

3. Можно ничего не менять в `NOTIFY_STORAGE`: по умолчанию стоит `auto`.
   Если `DATABASE_URL` есть, приложение само выберет PostgreSQL.

Для явного режима можно поставить:

```text
NOTIFY_STORAGE=postgres
```

## Диск Render

Если используешь Render Postgres, отдельный Persistent Disk для базы уведомлений не нужен.
PostgreSQL хранит данные в managed database.

Persistent Disk нужен только если ты хочешь сохранять локальные файлы web-service между деплоями. Для этого бота это не обязательно.

## Миграция старых данных

При первом запуске приложение автоматически мигрирует старые данные из:

```text
data/notify_subs.json
data/notify_matches.json
data/notify_state.sqlite3
```

в PostgreSQL.

Миграцию можно отключить переменными:

```text
NOTIFY_MIGRATE_JSON=0
NOTIFY_MIGRATE_SQLITE=0
```

## Проверка в логах

При старте должно быть:

```text
Storage: notify=postgres live_cache=sqlite
```

Если будет:

```text
Storage: notify=sqlite live_cache=sqlite
```

значит `DATABASE_URL` не подключён или пакет `psycopg` не установился.
