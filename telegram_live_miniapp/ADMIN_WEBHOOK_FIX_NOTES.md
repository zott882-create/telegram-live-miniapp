# Исправление Telegram-админ-панели v9.97

- Получение команд на публичном сервере переведено с `getUpdates` polling на Telegram webhook.
- На Render/Railway/Koyeb адрес webhook определяется автоматически из переменных платформы.
- Локально и на сервере без публичного HTTPS адреса используется резервный polling.
- Webhook защищён случайным путём на основе токена и заголовком `X-Telegram-Bot-Api-Secret-Token`.
- Общая обработка `message` и `callback_query` используется для webhook и polling.
- В разделе «Система» отображается активный транспорт: WEBHOOK или POLLING.

При необходимости адрес можно задать вручную переменной:

`PUBLIC_BASE_URL=https://your-domain.example`

Режим можно принудительно выбрать:

`ADMIN_UPDATE_MODE=auto|webhook|polling`
