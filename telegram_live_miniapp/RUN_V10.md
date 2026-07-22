# Быстрый запуск V10

1. Распакуйте архив.
2. Откройте терминал в папке `telegram_live_miniapp`.
3. Установите зависимости:

```bash
python -m pip install -r requirements.txt
```

4. Задайте переменные окружения по примеру `.env.example`.
5. Запустите:

```bash
python app.py
```

Для тестовой проверки интерфейса без Telegram-токена:

```bash
DATA_MODE=demo COLLECTOR_ENABLED=0 python app.py
```

На Windows переменные можно задать через `set`:

```bat
set DATA_MODE=demo
set COLLECTOR_ENABLED=0
python app.py
```
