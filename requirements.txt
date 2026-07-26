from pathlib import Path
import unittest
from unittest import mock
import datetime as dt

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


    def test_current_dom_uses_unique_event_id_and_keeps_duplicate_slugs(self):
        html = """
        <html><body><section class="match-state-section">
          <div class="table-active">
            <a href="/football/competition/argentina/argentine-division-1/abc/">Argentine Division 1</a>
            <a href="/football/country/argentina/">Argentina</a>
          </div>
          <div data-live-row data-match-id="event-new-1">
            <div class="football-match-table-container">
              <a class="sc-stretched-link" href="/football/match/banfield-vs-sarmiento-junin/"></a>
              <div data-utc="2030-07-28T15:00:00+00:00">15:00</div>
              <a class="sc-team-link" href="/football/team/banfield/ban-id/">Banfield</a>
              <a class="sc-team-link" href="/football/team/sarmiento-junin/sar-id/">Sarmiento Junin</a>
              <div data-market="tab-1"><span class="football-odd-cell">2.05</span><span class="football-odd-cell">3.00</span><span class="football-odd-cell">4.20</span></div>
            </div>
          </div>
          <div data-live-row data-match-id="event-new-2">
            <div class="football-match-table-container">
              <a class="sc-stretched-link" href="/football/match/banfield-vs-sarmiento-junin/"></a>
              <div data-utc="2031-07-28T15:00:00+00:00">15:00</div>
              <a class="sc-team-link" href="/football/team/banfield/ban-id/">Banfield</a>
              <a class="sc-team-link" href="/football/team/sarmiento-junin/sar-id/">Sarmiento Junin</a>
            </div>
          </div>
        </section></body></html>
        """
        rows = sportscore.parse_upcoming_html(html)
        self.assertEqual(len(rows), 2)
        self.assertEqual({x["id"] for x in rows}, {"ss:event-new-1", "ss:event-new-2"})
        first = next(x for x in rows if x["id"] == "ss:event-new-1")
        self.assertEqual(first["slug"], "banfield-vs-sarmiento-junin")
        self.assertEqual(first["home_link"], "https://sportscore.com/football/team/banfield/ban-id/")
        self.assertEqual(first["away_link"], "https://sportscore.com/football/team/sarmiento-junin/sar-id/")
        self.assertEqual(first["odds"]["eu"], {"1": 2.05, "X": 3.0, "2": 4.2})

    def test_list_upcoming_merges_by_event_id_not_slug(self):
        def page(event_id, kickoff):
            return f"""<html><body><section class="match-state-section">
            <div class="table-active"><a href="/football/competition/x/test/c/">Test League</a><a href="/football/country/x/">X</a></div>
            <div data-live-row data-match-id="{event_id}"><div class="football-match-table-container">
              <a class="sc-stretched-link" href="/football/match/a-vs-b/"></a>
              <div data-utc="{kickoff}">15:00</div>
              <a class="sc-team-link" href="/football/team/a/a-id/">A</a>
              <a class="sc-team-link" href="/football/team/b/b-id/">B</a>
            </div></div></section></body></html>"""

        pages = [page("event-a", "2030-07-28T15:00:00+00:00"), page("event-b", "2030-07-29T15:00:00+00:00")]
        calls = {"n": 0}
        def fake_html(path, ttl=0):
            value = pages[calls["n"] % len(pages)]
            calls["n"] += 1
            return value

        with mock.patch.object(sportscore, "_html", side_effect=fake_html), \
             mock.patch.object(sportscore, "PREMATCH_DAYS_AHEAD", 2), \
             mock.patch.object(sportscore, "PREMATCH_WORKERS", 1):
            rows = sportscore.list_upcoming(force=True)
        self.assertEqual({x["id"] for x in rows}, {"ss:event-a", "ss:event-b"})

    def test_detail_does_not_replace_future_fixture_with_old_finished_match(self):
        old_html = """<html><head><script type="application/ld+json">
        {"@context":"https://schema.org","@type":"SportsEvent",
         "url":"https://sportscore.com/football/match/banfield-vs-sarmiento-junin/",
         "name":"Banfield vs Sarmiento Junin",
         "startDate":"2020-07-26T15:00:00+00:00",
         "eventStatus":"https://schema.org/EventCompleted",
         "homeTeam":{"@type":"SportsTeam","name":"Banfield"},
         "awayTeam":{"@type":"SportsTeam","name":"Sarmiento Junin"}}
        </script></head><body></body></html>"""
        kickoff = int(dt.datetime(2030, 7, 28, 15, 0, tzinfo=dt.timezone.utc).timestamp())
        expected = {
            "id": "ss:event-new-1", "provider_match_id": "event-new-1",
            "slug": "banfield-vs-sarmiento-junin",
            "home": "Banfield", "away": "Sarmiento Junin",
            "home_id": "banfield", "away_id": "sarmiento-junin",
            "kickoff_at": kickoff, "kickoff_iso": "2030-07-28T15:00:00+00:00",
            "date_text": "28.07.2030", "time_text": "15:00",
            "league": "Argentine Division 1", "country": "Argentina",
            "status": "scheduled", "period": "PRE", "scheduled": True,
            "is_live": False, "finished": False,
            "odds": {"eu": {"1": 2.05, "X": 3.0, "2": 4.2}},
        }

        def fake_request(path, params=None, want_json=False):
            if str(path).startswith("/football/match/"):
                return old_html
            raise RuntimeError("offline in unit test")

        sportscore._CACHE.clear()
        with mock.patch.object(sportscore, "_request", side_effect=fake_request):
            payload = sportscore.detail(expected["slug"], force=True, expected_match=expected)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["detail_mismatch"])
        self.assertEqual(payload["match"]["id"], "ss:event-new-1")
        self.assertEqual(payload["match"]["kickoff_at"], kickoff)
        self.assertEqual(payload["match"]["period"], "PRE")
        self.assertFalse(payload["match"]["finished"])
        self.assertEqual(payload["match"]["score"], "0-0")
        self.assertEqual(payload["odds"]["eu"]["1"], 2.05)


    def test_internal_team_detail_calculates_statistics_without_external_links(self):
        now = int(dt.datetime(2030, 7, 30, tzinfo=dt.timezone.utc).timestamp())
        matches = [
            {
                "id": "m1", "home": "Wisla Krakow", "away": "Team A",
                "home_id": "wisla-krakow", "away_id": "team-a",
                "home_logo": "https://img.example/wisla.png", "away_logo": "",
                "score_home": 2, "score_away": 1, "finished": True,
                "scheduled": False, "is_live": False, "kickoff_at": now - 86400,
                "date_text": "29.07.2030", "time_text": "18:00",
                "league": "Test League", "country": "Poland",
                "link": "https://sportscore.com/football/match/wisla-vs-a/",
            },
            {
                "id": "m2", "home": "Team B", "away": "Wisla Krakow",
                "home_id": "team-b", "away_id": "wisla-krakow",
                "home_logo": "", "away_logo": "https://img.example/wisla.png",
                "score_home": 0, "score_away": 0, "finished": True,
                "scheduled": False, "is_live": False, "kickoff_at": now - 172800,
                "date_text": "28.07.2030", "time_text": "18:00",
                "league": "Test League", "country": "Poland",
                "link": "https://sportscore.com/football/match/b-vs-wisla/",
            },
            {
                "id": "m3", "home": "Wisla Krakow", "away": "Team C",
                "home_id": "wisla-krakow", "away_id": "team-c",
                "home_logo": "https://img.example/wisla.png", "away_logo": "",
                "score_home": 0, "score_away": 0, "finished": False,
                "scheduled": True, "is_live": False, "kickoff_at": now + 86400,
                "date_text": "31.07.2030", "time_text": "20:00",
                "league": "Test League", "country": "Poland",
                "link": "https://sportscore.com/football/match/wisla-vs-c/",
            },
        ]
        with mock.patch.object(sportscore, "_team_matches", return_value=matches), \
             mock.patch.object(sportscore.time, "time", return_value=now):
            payload = sportscore.team_detail("wisla-krakow", "Wisla Krakow", force=True)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["team"]["name"], "Wisla Krakow")
        self.assertEqual(payload["summary"]["count"], 2)
        self.assertEqual(payload["summary"]["wins"], 1)
        self.assertEqual(payload["summary"]["draws"], 1)
        self.assertEqual(payload["summary"]["losses"], 0)
        self.assertEqual(payload["summary"]["btts_percent"], 50)
        self.assertEqual(len(payload["upcoming"]), 1)
        for row in payload["recent"] + payload["upcoming"]:
            self.assertNotIn("link", row)
            self.assertNotIn("home_link", row)
            self.assertNotIn("away_link", row)

    def test_internal_team_detail_rejects_unsafe_slug(self):
        payload = sportscore.team_detail("https://sportscore.com/football/team/x/", "X", force=True)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "invalid_team_slug")


if __name__ == "__main__":
    unittest.main()
