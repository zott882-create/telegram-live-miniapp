# V10 — SportScore PREMATCH

## Что изменено

- На главном экране появились две вкладки: **LIVE** и **ПРЕМАТЧ**.
- LIVE по умолчанию продолжает работать через старый проверенный IGScore-модуль.
- Прематчи загружаются из SportScore.
- Добавлена карточка прематча с разделами:
  - Обзор;
  - H2H;
  - Таблица;
  - Составы.
- Из страницы матча читаются коэффициенты 1X2, тотал голов, тотал угловых,
  очные встречи, турнирная таблица, форма и составы.
- Временные cookies и `cf_clearance` не используются.

## Источники

- Список прематчей: `https://sportscore.com/football/?filter=upcoming`
- Матч: `https://sportscore.com/football/match/<slug>/`
- Live snapshot: `https://sportscore.com/football/match/<slug>/live/`
- Публичные widget API используются как дополнительный источник, если доступны.

## Настройка

Рекомендуемая конфигурация на первом запуске:

```env
LIVE_PROVIDER=igscore
PREMATCH_ENABLED=1
```

После проверки SportScore LIVE на вашем сервере можно включить:

```env
LIVE_PROVIDER=sportscore
```

## Проверка

```bash
python -m unittest discover -s tests -v
python app.py
```

Проверить в браузере:

- `/health`
- `/api/live`
- `/api/prematch`
- главную страницу Mini App

## Важно

HTML SportScore может меняться. Парсер написан с несколькими вариантами поиска
данных, но при изменении верстки его селекторы может потребоваться обновить.
На странице прематчей сохранена видимая ссылка `Powered by SportScore`.
