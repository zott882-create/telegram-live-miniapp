Что добавлено в эту сборку
==========================

1) В app.py добавлен backend endpoint:
   POST /api/value-bet

Пример body:
{
  "odds": 2.10,
  "probability_percent": 52,
  "bankroll": 1000
}

Ответ возвращает:
- implied probability букмекера
- fair odds
- EV %
- Kelly %
- 1/2 Kelly и 1/4 Kelly
- рекомендуемый размер ставки, если указан bankroll

2) В static/index.html добавлена карточка "📊 Value Bet" на экране деталей матча.
   Она появляется под блоком "Лайв КФ".

3) В static/styles.css добавлены стили для карточки Value Bet.

Важно:
- модуль ничего не ставит автоматически;
- это математический расчёт, а не гарантия выигрыша;
- лучше использовать 1/4 Kelly или 1/2 Kelly, а не полный Kelly.
