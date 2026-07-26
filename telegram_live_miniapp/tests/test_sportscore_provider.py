from pathlib import Path
import datetime as dt
import time
import unittest
from unittest import mock

from providers import sportscore

FIXTURES = Path(__file__).parent / "fixtures"


class SportScoreParserTests(unittest.TestCase):
    def test_match_page_parses_prematch_data(self):
        html = (FIXTURES / "sportscore_match_palmeiras_coritiba.html").read_text("utf-8")
        payload = sportscore.parse_match_html(html, "palmeiras-vs-coritiba-pr")

        match = payload["match"]
        self.assertEqual(match["id"], "ss:palmeiras-vs-coritiba-pr")
        # Do not trust slug order: structured home/away data is authoritative.
        self.assertEqual(match["home"], "Coritiba SAF - PR")
        self.assertEqual(match["away"], "Palmeiras - SP")
        self.assertEqual(match["league"], "Brazilian Serie A")
        self.assertEqual(match["competition_slug"], "brazilian-serie-a")

        odds = payload["odds"]
        self.assertEqual(odds["eu"], {"1": 4.2, "X": 3.4, "2": 1.9})
        self.assertEqual(odds["bs"]["line"], 2.25)
        self.assertEqual(odds["corners"]["line"], 10.5)

        h2h = payload["h2h"]
        self.assertEqual(h2h["total"], 30)
        self.assertEqual(h2h["home_wins"], 12)
        self.assertEqual(h2h["draws"], 6)
        self.assertEqual(h2h["away_wins"], 12)
        self.assertGreaterEqual(len(h2h["meetings"]), 5)

        table = payload["standings"]
        self.assertGreaterEqual(len(table), 20)
        palmeiras = next(x for x in table if x["team"] == "Palmeiras - SP")
        coritiba = next(x for x in table if x["team"] == "Coritiba SAF - PR")
        self.assertEqual(palmeiras["position"], 1)
        self.assertEqual(palmeiras["points"], 41)
        self.assertEqual(coritiba["position"], 7)
        self.assertEqual(coritiba["points"], 26)

        self.assertFalse(payload["lineups"]["announced"])
        self.assertIn("not announced", payload["lineups"]["message"].lower())
        self.assertEqual(payload["venue"]["name"], "Estádio Couto Pereira")
        self.assertEqual(payload["venue"]["capacity"], 40502)

    def test_upcoming_page_parses_scheduled_matches(self):
        html = (FIXTURES / "sportscore_upcoming.html").read_text("utf-8")
        rows = sportscore.parse_upcoming_html(html)
        self.assertGreaterEqual(len(rows), 15)
        match = next(x for x in rows if x["slug"] == "palmeiras-vs-coritiba-pr")
        self.assertEqual(match["home"], "Coritiba SAF - PR")
        self.assertEqual(match["away"], "Palmeiras - SP")
        self.assertTrue(match["scheduled"])
        self.assertEqual(match["id"], "ss:palmeiras-vs-coritiba-pr")

    def test_upcoming_dom_parser_does_not_stop_at_seo_sample(self):
        rows = []
        for i in range(101):
            rows.append(f"""
            <div class="football-match-table-container sc-row-stretched">
              <a class="sc-stretched-link" href="/football/match/home-{i}-vs-away-{i}/"></a>
              <a href="/football/competition/test/test-league/test-id/">Test League</a>
              <a href="/football/team/home-{i}/h{i}/">Home {i}</a>
              <a href="/football/team/away-{i}/a{i}/">Away {i}</a>
              <time datetime="2026-07-22T17:{i % 60:02d}:00+00:00">17:{i % 60:02d}</time>
            </div>
            """)
        html = "<html><body><h2>Scheduled fixtures 101</h2>" + "".join(rows) + "</body></html>"
        parsed = sportscore.parse_upcoming_html(html)
        self.assertEqual(len(parsed), 101)
        self.assertEqual(parsed[0]["league"], "Test League")

    def test_upcoming_parser_uses_scheduled_section_only(self):
        future = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=4)).isoformat()
        past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2)).isoformat()

        def row(slug, home, away, kickoff):
            return f"""
            <div class="football-match-table-container match-row">
              <a class="sc-stretched-link" href="/football/match/{slug}/"></a>
              <a href="/football/competition/test/test-league/test-id/">Test League</a>
              <a href="/football/team/{home.lower()}/h/">{home}</a>
              <a href="/football/team/{away.lower()}/a/">{away}</a>
              <time datetime="{kickoff}">20:00</time>
            </div>
            """

        html = f"""
        <html><body>
          <h2>Live football matches 1</h2>
          {row('live-home-vs-live-away', 'Live Home', 'Live Away', past)}
          <h2>Scheduled fixtures 1</h2>
          {row('future-home-vs-future-away', 'Future Home', 'Future Away', future)}
          <h2>Finished matches 1</h2>
          {row('old-home-vs-old-away', 'Old Home', 'Old Away', past)}
        </body></html>
        """
        parsed = sportscore.parse_upcoming_html(html)
        self.assertEqual([x["slug"] for x in parsed], ["future-home-vs-future-away"])

    def test_list_upcoming_fetches_date_pages_in_parallel(self):
        def fake_html(path, ttl):
            time.sleep(0.15)
            date = path.split("date=", 1)[1].split("&", 1)[0]
            kickoff = f"{date}T23:50:00+00:00"
            slug = "match-" + date
            return f"""
            <html><body><h2>Scheduled fixtures 1</h2>
              <div class="football-match-table-container match-row">
                <a href="/football/match/{slug}/"></a>
                <a href="/football/competition/test/test-league/test-id/">Test League</a>
                <a href="/football/team/home/h/">Home {date}</a>
                <a href="/football/team/away/a/">Away {date}</a>
                <time datetime="{kickoff}">23:50</time>
              </div>
            </body></html>
            """

        with mock.patch.object(sportscore, "PREMATCH_DAYS_AHEAD", 3), \
             mock.patch.object(sportscore, "PREMATCH_WORKERS", 3), \
             mock.patch.object(sportscore, "_html", side_effect=fake_html):
            started = time.monotonic()
            rows = sportscore.list_upcoming(force=True)
            elapsed = time.monotonic() - started

        self.assertEqual(len(rows), 3)
        # Sequential loading would be around 0.45s before parsing overhead.
        self.assertLess(elapsed, 0.38)

    def test_list_upcoming_strictly_removes_live_finished_and_past(self):
        now = int(time.time())
        rows = [
            {"slug": "future", "scheduled": True, "kickoff_at": now + 3600, "is_live": False, "finished": False},
            {"slug": "past", "scheduled": True, "kickoff_at": now - 3600, "is_live": False, "finished": False},
            {"slug": "live", "scheduled": False, "kickoff_at": now + 3600, "is_live": True, "finished": False},
            {"slug": "finished", "scheduled": False, "kickoff_at": now + 3600, "is_live": False, "finished": True},
            {"slug": "unknown-time", "scheduled": True, "kickoff_at": 0, "is_live": False, "finished": False},
        ]
        with mock.patch.object(sportscore, "_html", side_effect=RuntimeError("no html")), \
             mock.patch.object(sportscore, "list_api_matches", return_value=rows):
            result = sportscore.list_upcoming(force=True)
        self.assertEqual([x["slug"] for x in result], ["future"])



if __name__ == "__main__":
    unittest.main()
