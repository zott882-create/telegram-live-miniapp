from pathlib import Path
import unittest

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


if __name__ == "__main__":
    unittest.main()
