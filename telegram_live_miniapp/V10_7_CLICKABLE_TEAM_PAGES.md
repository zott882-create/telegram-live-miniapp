# V10.7 — clickable SportScore team pages

- Preserves the real home and away team URLs from SportScore HTML/JSON-LD.
- Team logo and team name on the prematch detail scoreboard are clickable.
- Telegram Mini App opens the selected team in the external browser.
- Regular browsers use a safe `target="_blank"` fallback.
- Links are restricted to `https://sportscore.com/football/team/`.
- Live match collection and prematch fixture identity logic are unchanged.
