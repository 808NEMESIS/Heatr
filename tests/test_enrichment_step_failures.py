"""
tests/test_enrichment_step_failures.py — H4 (audit v2): stil-gefaalde
enrichment-stappen worden zichtbaar gemaakt.

Vóór deze fix rapporteerde elke enrichment-job 'completed', ook als een
kritieke stap (bv. email_waterfall) non-fataal crashte — datagaten bleven
onzichtbaar. Nu markeert completion een job met gefaalde stappen als
'completed_with_errors' + error_message.

Run met: pytest tests/test_enrichment_step_failures.py -v
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from job_queue import enrichment_queue as eq


def _captured_update(db: MagicMock) -> dict:
    """Haal het update-payload uit de mock-chain
    table(...).update(<payload>).eq(...).execute()."""
    call = db.table.return_value.update.call_args
    assert call is not None, "update() is nooit aangeroepen"
    return call.args[0]


class TestCompleteWithoutFailures:
    @pytest.mark.asyncio
    async def test_no_failures_marks_completed(self):
        db = MagicMock()
        await eq.complete_enrichment_job("job-1", db)
        payload = _captured_update(db)
        assert payload["status"] == "completed"
        assert "error_message" not in payload
        assert "completed_at" in payload

    @pytest.mark.asyncio
    async def test_empty_list_marks_completed(self):
        db = MagicMock()
        await eq.complete_enrichment_job("job-1", db, steps_failed=[])
        assert _captured_update(db)["status"] == "completed"


class TestCompleteWithFailures:
    @pytest.mark.asyncio
    async def test_failures_mark_completed_with_errors(self):
        db = MagicMock()
        failed = [
            {"step": "email_waterfall", "error": "SMTP timeout"},
            {"step": "owner_extract", "error": "no page_text"},
        ]
        await eq.complete_enrichment_job("job-1", db, steps_failed=failed)
        payload = _captured_update(db)
        assert payload["status"] == "completed_with_errors"
        # error_message noemt beide gefaalde stappen + hun reden
        msg = payload["error_message"]
        assert "email_waterfall" in msg and "SMTP timeout" in msg
        assert "owner_extract" in msg
        assert msg.startswith("2 stap(pen) non-fataal gefaald")

    @pytest.mark.asyncio
    async def test_error_message_capped_at_2000(self):
        db = MagicMock()
        failed = [{"step": f"step_{i}", "error": "x" * 200} for i in range(50)]
        await eq.complete_enrichment_job("job-1", db, steps_failed=failed)
        assert len(_captured_update(db)["error_message"]) <= 2000

    @pytest.mark.asyncio
    async def test_db_failure_is_swallowed(self):
        """Completion mag nooit crashen op een DB-fout (fail-soft)."""
        db = MagicMock()
        db.table.return_value.update.return_value.eq.return_value.execute.side_effect = (
            RuntimeError("db gone")
        )
        # mag niet raisen
        await eq.complete_enrichment_job("job-1", db, steps_failed=[{"step": "x", "error": "y"}])
