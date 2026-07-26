import unittest
from unittest import mock

import app


class InternalTeamPageTests(unittest.TestCase):
    def setUp(self):
        self.row = {
            "id": "ss:event-team-test",
            "provider_match_id": "event-team-test",
            "slug": "wisla-krakow-vs-gks-katowice",
            "home": "Wisla Krakow",
            "away": "GKS Katowice",
            "home_id": "wisla-krakow",
            "away_id": "gks-katowice",
            "home_logo": "https://img.example/wisla.png",
            "away_logo": "https://img.example/gks.png",
            "home_link": "https://sportscore.com/football/team/wisla-krakow/x/",
            "away_link": "https://sportscore.com/football/team/gks-katowice/y/",
            "link": "https://sportscore.com/football/match/wisla-vs-gks/",
            "country": "Poland",
            "country_code": "PL",
            "league": "Ekstraklasa",
            "kickoff_at": 2_100_000_000,
            "period": "PRE",
            "status": "scheduled",
            "scheduled": True,
            "is_live": False,
            "finished": False,
        }

    def test_prematch_public_payload_does_not_expose_provider_links(self):
        payload = app._prematch_payload_from_rows([self.row])
        rows = app.flatten_payload(payload)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["home_id"], "wisla-krakow")
        self.assertEqual(row["away_id"], "gks-katowice")
        for key in ("link", "home_link", "away_link", "country_url", "competition_url"):
            self.assertNotIn(key, row)
        self.assertNotIn("attribution", payload)

    def test_team_payload_is_selected_by_internal_match_id_and_side(self):
        with app._cache_lock:
            app._prematch_cache["payload"] = app._prematch_payload_from_rows([self.row])
            app._prematch_cache["saved_at"] = 1.0
        fake = {
            "ok": True,
            "team": {"id": "wisla-krakow", "name": "Wisla Krakow"},
            "summary": {"count": 10},
            "splits": {"home": {}, "away": {}},
            "recent": [],
            "upcoming": [],
        }
        with mock.patch.object(app.sportscore_provider, "team_detail", return_value=fake) as called:
            payload = app.sportscore_team_payload("ss:event-team-test", "home")
        self.assertTrue(payload["ok"])
        called.assert_called_once_with("wisla-krakow", "Wisla Krakow")
        self.assertEqual(payload["team"]["logo"], "https://img.example/wisla.png")
        self.assertEqual(payload["context"]["opponent"], "GKS Katowice")
        self.assertNotIn("url", payload["team"])

    def test_wsgi_team_endpoint_returns_internal_payload(self):
        fake = {"ok": True, "team": {"id": "wisla-krakow", "name": "Wisla Krakow"}, "recent": [], "upcoming": []}
        captured = {}
        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = headers
        environ = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": "/api/team",
            "QUERY_STRING": "match_id=ss%3Aevent-team-test&side=home",
            "REMOTE_ADDR": "127.0.0.1",
            "wsgi.input": __import__("io").BytesIO(b""),
            "CONTENT_LENGTH": "0",
        }
        with mock.patch.object(app, "_rate_limit_ok", return_value=True), \
             mock.patch.object(app, "sportscore_team_payload", return_value=fake) as called:
            body = b"".join(app.application(environ, start_response))
        self.assertTrue(captured["status"].startswith("200"))
        self.assertIn(b'"ok":true', body)
        called.assert_called_once_with("ss:event-team-test", "home")


if __name__ == "__main__":
    unittest.main()
