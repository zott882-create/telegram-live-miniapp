# Патч флагов лиг

Добавлены принудительные алиасы стран/флагов для лиг, которые IGScore иногда отдаёт в общих регионах `EUROPE`, `AMERICAS`, `OTHERS` без `country_code`.

Добавлено:

- `FRA WD2` → Франция / `FR`
- `ARFC` → Аргентина / `AR`
- `Italy Amateur Eccellenza Puglia` → Италия / `IT`
- `BCU20 Women` → Бразилия / `BR`

Изменённые файлы:

- `app.py`
- `static/index.html`

После загрузки в GitHub/Railway достаточно сделать обычный redeploy. Переменные Railway, Postgres, Redis и домен перенастраивать не нужно.
