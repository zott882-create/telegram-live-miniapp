# V10.6 — full prematch + unique fixture ID

## Fixed

- Replaced the quadratic SportScore DOM crawler with a direct linear parser of `match-state-section` rows.
- Reads the real fixture identity from `data-match-id`; slugs remain routes only.
- Keeps separate fixtures even when SportScore reuses the same team-pair slug.
- Opens the selected prematch by looking it up in the current prematch cache.
- Validates the detail page against the selected teams and kickoff. An old/finished slug result can no longer replace a future fixture.
- Uses the selected list row as the authoritative source for fixture ID, kickoff, teams and odds.
- Parses 1X2, Asian handicap, totals and corners from their own market blocks.
- Keeps the detail card usable when SportScore detail/team endpoints are temporarily unavailable.

## Verification

- Current saved SportScore HTML: 53 rendered rows parsed in under one second in the local check.
- Five parser/detail regression tests pass.
- All Python modules compile.
