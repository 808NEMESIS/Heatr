"""tests/test_inbox_recovery.py — H5 queued_no_inbox recovery (dry-run + idempotent)."""
import sys, asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from job_queue.inbox_recovery import recover_queued_no_inbox


class _Q:
    def __init__(self, db): self.db=db; self.op="select"; self.payload=None; self.f={}
    def select(self,*a,**k): return self
    def update(self,p): self.op,self.payload="update",p; return self
    def eq(self,c,v): self.f[c]=v; return self
    def limit(self,n): return self
    def execute(self):
        R=type("R",(),{})()
        if self.op=="update":
            for l in self.db.leads:
                if all(l.get(c)==v for c,v in self.f.items()): l.update(self.payload)
            R.data=[]; return R
        R.data=[dict(l) for l in self.db.leads if l.get("status")=="queued_no_inbox"]; return R

class _DB:
    def __init__(self,leads): self.leads=leads
    def table(self,n): return _Q(self)

def _run(c): return asyncio.new_event_loop().run_until_complete(c)

def _patch_inbox(inboxes):
    return (
        patch("job_queue.enrichment_queue._get_cached_inboxes", new=AsyncMock(return_value=inboxes)),
        patch("job_queue.enrichment_queue._detect_email_provider", return_value="google"),
        patch("job_queue.enrichment_queue._select_best_inbox", return_value=(inboxes[0] if inboxes else None)),
    )

def test_dry_run_writes_nothing():
    db=_DB([{"id":"1","workspace_id":"aerys","status":"queued_no_inbox","email":"a@b.nl","email_status":"risky"}])
    p1,p2,p3=_patch_inbox([{"id":"ibx-1"}])
    with p1,p2,p3:
        r=_run(recover_queued_no_inbox(db,"aerys",object(),dry_run=True))
    assert r["would_link"]==1 and r["distribution"]=={"ibx-1":1}
    assert db.leads[0]["status"]=="queued_no_inbox"  # niets geschreven

def test_no_inboxes_leaves_unlinked():
    db=_DB([{"id":"1","workspace_id":"aerys","status":"queued_no_inbox","email":"a@b.nl","email_status":"risky"}])
    p1,p2,p3=_patch_inbox([])
    with p1,p2,p3:
        r=_run(recover_queued_no_inbox(db,"aerys",object(),dry_run=True))
    assert r["would_link"]==0 and r["unlinked"]==1

def test_apply_updates_status():
    db=_DB([{"id":"1","workspace_id":"aerys","status":"queued_no_inbox","email":"a@b.nl","email_status":"valid"}])
    p1,p2,p3=_patch_inbox([{"id":"ibx-1"}])
    with p1,p2,p3:
        _run(recover_queued_no_inbox(db,"aerys",object(),dry_run=False))
    assert db.leads[0]["status"]=="qualified"
    assert db.leads[0]["preferred_inbox_id"]=="ibx-1"

def test_idempotent_second_run_finds_nothing():
    # lead al hersteld → status niet meer queued → recovery vindt 'm niet
    db=_DB([{"id":"1","status":"qualified","email":"a@b.nl","email_status":"valid"}])
    p1,p2,p3=_patch_inbox([{"id":"ibx-1"}])
    with p1,p2,p3:
        r=_run(recover_queued_no_inbox(db,"aerys",object(),dry_run=False))
    assert r["total"]==0
