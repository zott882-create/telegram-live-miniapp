from unittest import mock
import io
import json

import app
from providers import sportscore


COMP_HTML = r'''
<html><head><title>Test League</title></head><body>
<a href="/football/country/poland/">Poland</a>
<div class="comp-hero"><img src="https://img.thesports.com/football/competition/test.png"><h1>Test League — Football Standings, Fixtures & Results</h1></div>
<div>12 teams 132 matches 22 rounds</div>
<select id="season-select" name="season"><option value="2026" selected>2026</option><option value="2025">2025</option></select>
<button data-round="1">Round 1</button><button data-round="2">Round 2</button>
<section class="match-state-section">
  <div class="table-active"><a href="/football/country/poland/">Poland</a><a href="/football/competition/poland/test-league/abc123/">Test League</a></div>
  <div>Round 1</div>
  <div data-live-row data-match-id="match001">
    <div class="d-none d-md-flex"><div class="football-match-table-container">
      <a class="sc-stretched-link" href="/football/match/alpha-vs-beta/"></a>
      <div data-utc="2026-08-01T18:00:00+00:00"></div><div data-live="status"></div>
      <div class="football-match-table-box">
        <img class="icon-circuit" src="https://img.thesports.com/football/team/a.png">
        <a class="sc-team-link" href="/football/team/alpha/alpha123/">Alpha FC</a>
        <span data-live="home-score"><b></b></span><span data-live="away-score"><b></b></span>
        <img class="icon-circuit" src="https://img.thesports.com/football/team/b.png">
        <a class="sc-team-link" href="/football/team/beta/beta123/">Beta FC</a>
      </div>
    </div></div>
  </div>
  <div data-live-row data-match-id="match002">
    <div class="d-none d-md-flex"><div class="football-match-table-container">
      <a class="sc-stretched-link" href="/football/match/gamma-vs-delta/"></a>
      <div data-utc="2026-07-01T18:00:00+00:00"></div><div data-live="status">FT</div>
      <div class="football-match-table-box">
        <img class="icon-circuit" src="https://img.thesports.com/football/team/c.png">
        <a class="sc-team-link" href="/football/team/gamma/gamma123/">Gamma FC</a>
        <span data-live="home-score"><b>2</b></span><span data-live="away-score"><b>1</b></span>
        <img class="icon-circuit" src="https://img.thesports.com/football/team/d.png">
        <a class="sc-team-link" href="/football/team/delta/delta123/">Delta FC</a>
      </div>
    </div></div>
  </div>
</section>
<section id="standings"><h2>Standings</h2><table class="standings-table"><tbody>
<tr><td>1</td><td><a href="/football/team/alpha/alpha123/"><img src="https://img.thesports.com/football/team/a.png">Alpha FC</a></td><td>10</td><td>7</td><td>2</td><td>1</td><td>+12</td><td>23</td></tr>
<tr><td>2</td><td><a href="/football/team/beta/beta123/">Beta FC</a></td><td>10</td><td>6</td><td>2</td><td>2</td><td>+8</td><td>20</td></tr>
</tbody></table></section>
<section><h2>Top scorers</h2><table><tbody><tr><td><a href="/football/player/john-doe/player123/">John Doe</a></td><td><a href="/football/team/alpha/alpha123/">Alpha FC</a></td><td>9</td></tr></tbody></table></section>
<section><h2>Top assists</h2><ul><li><a href="/football/player/max-pass/pass123/">Max Pass</a> <a href="/football/team/beta/beta123/">Beta FC</a> 7</li></ul></section>
<section><h2>Top rated</h2><ul><li><a href="/football/player/star-man/star123/">Star Man</a> <a href="/football/team/alpha/alpha123/">Alpha FC</a> 8.1</li></ul></section>
<section><h2>Champions</h2><ul><li>2025 <a href="/football/team/alpha/alpha123/">Alpha FC</a></li></ul></section>
</body></html>
'''


def test_competition_detail_builds_full_internal_centre():
    with mock.patch.object(sportscore, "_request", return_value=COMP_HTML):
        data = sportscore.competition_detail("/football/competition/poland/test-league/abc123/", force=True)
    assert data["ok"] is True
    assert data["competition"]["name"] == "Test League"
    assert data["competition"]["id"] == "abc123"
    assert len(data["matches"]) == 2
    assert len(data["upcoming"]) == 1
    assert len(data["results"]) == 1
    assert data["results"][0]["score_home"] == 2
    assert data["standings"][0]["team_id"] == "alpha"
    assert data["players"]["scorers"][0]["name"] == "John Doe"
    assert data["players"]["assists"][0]["value"] == 7
    assert data["players"]["ratings"][0]["value"] == 8.1
    assert data["teams"][0]["name"] == "Alpha FC"
    assert any(x["value"] == "1" for x in data["round_options"])
    assert data["champions"][0]["team_id"] == "alpha"


def test_competition_payload_removes_external_navigation():
    fake = {
        "ok": True,
        "provider": "sportscore",
        "competition": {"path": "/football/competition/poland/test-league/abc123/", "country": "Poland", "url": "https://sportscore.com/x"},
        "matches": [{"id": "ss:1", "home": "A", "away": "B", "link": "https://sportscore.com/m", "competition_path": "/football/competition/poland/test-league/abc123/"}],
        "upcoming": [], "results": [], "live": [], "bracket": [],
    }
    with mock.patch.object(app.sportscore_provider, "competition_detail", return_value=fake):
        out = app.sportscore_competition_payload("/football/competition/poland/test-league/abc123/")
    assert out["ok"] is True
    assert "url" not in out["competition"]
    assert "link" not in out["matches"][0]
    assert out["matches"][0]["competition_path"].startswith("/football/competition/")



def _wsgi_get(path, query=""):
    captured = {}
    environ = {
        "REQUEST_METHOD": "GET",
        "PATH_INFO": path,
        "QUERY_STRING": query,
        "REMOTE_ADDR": "127.0.0.1",
        "wsgi.input": io.BytesIO(b""),
    }
    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers
    body = b"".join(app.application(environ, start_response))
    return captured["status"], json.loads(body.decode("utf-8"))


def test_competition_rows_are_reused_by_exact_event_id_for_match_detail():
    fake = {
        "ok": True,
        "competition": {"path": "/football/competition/poland/test-league/abc123/", "country": "Poland"},
        "matches": [{
            "id": "ss:event-exact-77",
            "slug": "alpha-vs-beta",
            "home": "Alpha FC",
            "away": "Beta FC",
            "home_id": "alpha",
            "away_id": "beta",
            "kickoff_at": 1785607200,
        }],
        "upcoming": [], "results": [], "live": [], "bracket": [],
    }
    with mock.patch.object(app.sportscore_provider, "competition_detail", return_value=fake):
        out = app.sportscore_competition_payload("/football/competition/poland/test-league/abc123/")
    assert out["ok"] is True
    row = app._prematch_row_by_id("ss:event-exact-77")
    assert row is not None
    assert row["slug"] == "alpha-vs-beta"
    assert row["kickoff_at"] == 1785607200
    with mock.patch.object(app.sportscore_provider, "detail", return_value={"ok": True}) as detail:
        app.sportscore_detail_payload("ss:event-exact-77")
    detail.assert_called_once()
    assert detail.call_args.args[0] == "alpha-vs-beta"
    assert detail.call_args.kwargs["expected_match"]["id"] == "ss:event-exact-77"


def test_competition_and_direct_team_wsgi_routes():
    competition_payload = {"ok": True, "competition": {"name": "Test League"}}
    team_payload = {"ok": True, "team": {"id": "alpha", "name": "Alpha FC"}}
    with mock.patch.object(app, "_rate_limit_ok", return_value=True), \
         mock.patch.object(app, "sportscore_competition_payload", return_value=competition_payload) as competition_call, \
         mock.patch.object(app, "sportscore_team_direct_payload", return_value=team_payload) as team_call:
        status, body = _wsgi_get(
            "/api/competition",
            "path=%2Ffootball%2Fcompetition%2Fpoland%2Ftest-league%2Fabc123%2F&season=2026&round=2",
        )
        assert status.startswith("200")
        assert body["competition"]["name"] == "Test League"
        competition_call.assert_called_once_with(
            "/football/competition/poland/test-league/abc123/", "2026", "2", force=False
        )

        status, body = _wsgi_get(
            "/api/team",
            "team_id=alpha&team_name=Alpha%20FC&competition_path=%2Ffootball%2Fcompetition%2Fpoland%2Ftest-league%2Fabc123%2F",
        )
        assert status.startswith("200")
        assert body["team"]["name"] == "Alpha FC"
        team_call.assert_called_once_with(
            "alpha", "Alpha FC", "/football/competition/poland/test-league/abc123/"
        )
