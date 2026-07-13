"""tests/test_verify_api.py — Bouncer-mapping naar onze granulaire enum."""
import sys, asyncio
from pathlib import Path
from unittest.mock import patch, MagicMock
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from enrichment import verify_api

def _mock_get(status_code=200, json_body=None):
    resp = MagicMock(); resp.status_code=status_code
    resp.json=lambda: (json_body or {}); resp.raise_for_status=lambda: None
    client = MagicMock()
    async def _get(*a,**k): return resp
    client.get=_get
    cm = MagicMock(); cm.__aenter__=lambda s: _acm(client); cm.__aexit__=lambda s,*a: _acm(None)
    return cm
async def _acm(v): return v

def _run(c): return asyncio.new_event_loop().run_until_complete(c)

def _call(json_body, code=200, key="k", prov="bouncer"):
    with patch.dict("os.environ", {"BOUNCER_API_KEY":key,"EMAIL_VERIFY_PROVIDER":prov}), \
         patch("enrichment.verify_api.httpx.AsyncClient", return_value=_mock_get(code,json_body)):
        return _run(verify_api.verify_via_api("a@b.nl"))

def test_deliverable_to_valid():
    assert _call({"status":"deliverable","domain":{"acceptAll":"no"}})["status"]=="valid"
def test_undeliverable_to_invalid():
    assert _call({"status":"undeliverable","domain":{"acceptAll":"no"}})["status"]=="invalid"
def test_acceptall_to_catchall():
    assert _call({"status":"deliverable","domain":{"acceptAll":"yes"}})["status"]=="catchall_risky"
def test_risky_stays_risky():
    assert _call({"status":"risky","domain":{"acceptAll":"no"}})["status"]=="risky"
def test_unknown_to_not_checked():
    assert _call({"status":"unknown","domain":{"acceptAll":"no"}})["status"]=="not_checked"
def test_402_no_credits_fail_closed():
    r=_call({}, code=402)
    assert r["status"]=="not_checked" and "402" in r.get("raw_reason","")
def test_disabled_when_no_provider():
    with patch.dict("os.environ", {"EMAIL_VERIFY_PROVIDER":"none"}, clear=False):
        assert _run(verify_api.verify_via_api("a@b.nl"))["method"]=="disabled"
def test_bad_email_format():
    assert _run(verify_api.verify_via_api("geenmail"))["status"]=="invalid"
