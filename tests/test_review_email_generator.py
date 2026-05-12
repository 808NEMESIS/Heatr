"""
tests/test_review_email_generator.py — Tests for generate_review_email.

Verifies prompt/output shape, error handling, and never-raise contract.
Run with: pytest tests/test_review_email_generator.py -v
"""
from __future__ import annotations

import json
import os
import pytest
from unittest.mock import AsyncMock, MagicMock

from campaigns.review_email_generator import (
    _compose_specific_observation,
    _derive_top_issue,
    generate_review_email,
)


def _fake_claude_response(subject: str, body: str, in_tok: int = 400, out_tok: int = 120):
    """Build an object shaped like anthropic.AsyncAnthropic.messages.create result."""
    payload = json.dumps({"subject": subject, "body": body})
    content_block = MagicMock()
    content_block.text = payload
    usage = MagicMock()
    usage.input_tokens = in_tok
    usage.output_tokens = out_tok
    resp = MagicMock()
    resp.content = [content_block]
    resp.usage = usage
    return resp


def _make_anthropic_client(response):
    client = MagicMock()
    client.messages = MagicMock()
    client.messages.create = AsyncMock(return_value=response)
    return client


# ==============================================================================
# Top-issue derivation (rule-based, no Claude)
# ==============================================================================

class TestDeriveTopIssue:
    def test_empty_wi_returns_default(self):
        assert _derive_top_issue({}) == "website uit de tijd"

    def test_low_technical_score_wins(self):
        wi = {"technical_score": 5, "conversion_score": 20, "sector_score": 10}
        assert _derive_top_issue(wi) == "verouderde techniek"

    def test_low_conversion_no_booking(self):
        wi = {
            "technical_score": 20,
            "conversion_score": 5,
            "conversion_details": {"has_online_booking": False, "has_whatsapp": True, "has_chatbot": True},
        }
        assert _derive_top_issue(wi) == "geen online boekingsmogelijkheid"

    def test_low_conversion_no_whatsapp(self):
        wi = {
            "technical_score": 20,
            "conversion_score": 5,
            "conversion_details": {"has_online_booking": True, "has_whatsapp": False, "has_chatbot": True},
        }
        assert _derive_top_issue(wi) == "geen WhatsApp voor klanten"

    def test_low_sector_score_first_failing_check(self):
        wi = {
            "technical_score": 20, "conversion_score": 20, "sector_score": 3,
            "sector_details": {
                "checks": [
                    {"label": "BIG-registratie zichtbaar", "passed": False, "points": 5},
                    {"label": "Behandelmenu", "passed": True, "points": 5},
                ]
            },
        }
        assert _derive_top_issue(wi) == "BIG-registratie zichtbaar"


class TestComposeSpecificObservation:
    def test_tech_without_ssl(self):
        wi = {"technical_details": {"has_ssl": False}}
        assert "SSL" in _compose_specific_observation(wi, "verouderde techniek")

    def test_no_booking(self):
        wi = {}
        out = _compose_specific_observation(wi, "geen online boekingsmogelijkheid")
        assert "bellen" in out or "mailen" in out


# ==============================================================================
# generate_review_email — end-to-end with mocked Claude
# ==============================================================================

class TestGenerateReviewEmail:
    @pytest.mark.asyncio
    async def test_incomplete_lead_returns_error_no_raise(self):
        result = await generate_review_email(
            lead={"city": "Amsterdam"},  # missing company_name
            website_intelligence={"total_score": 40},
        )
        assert isinstance(result, dict)
        assert result["error"].startswith("incomplete_lead_data")
        assert result["subject"] == ""
        assert result["body"] == ""

    @pytest.mark.asyncio
    async def test_missing_api_key_returns_error(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        result = await generate_review_email(
            lead={"company_name": "Testpraktijk", "city": "Utrecht"},
            website_intelligence={"total_score": 35},
        )
        assert result["error"] == "ANTHROPIC_API_KEY missing"
        assert result["top_issue"]  # derived even without Claude

    @pytest.mark.asyncio
    async def test_happy_path_returns_parsed_email(self):
        resp = _fake_claude_response(
            subject="Uw website scoort lager dan concurrenten",
            body="Opgevallen dat de site geen online boeking heeft. Welke vraag laat u nu liggen?",
        )
        client = _make_anthropic_client(resp)

        result = await generate_review_email(
            lead={"company_name": "Praktijk Zen", "city": "Amsterdam", "contact_first_name": "Eva"},
            website_intelligence={
                "total_score": 35,
                "technical_score": 20,
                "conversion_score": 5,
                "conversion_details": {"has_online_booking": False, "has_whatsapp": True, "has_chatbot": True},
                "competitor_data": {"score_vs_market": -15},
            },
            anthropic_client=client,
        )
        assert "error" not in result
        assert result["subject"].startswith("Uw website")
        assert result["body"]
        assert "?" in result["body"]  # one concrete question
        assert result["top_issue"] == "geen online boekingsmogelijkheid"
        assert result["tokens_used"] == 520
        assert result["cost_eur"] > 0

    @pytest.mark.asyncio
    async def test_body_starting_with_ik_is_corrected(self):
        resp = _fake_claude_response(
            subject="Observatie over uw website",
            body="Ik viel op dat de site langzaam laadt. Is dat bewust?",
        )
        client = _make_anthropic_client(resp)
        result = await generate_review_email(
            lead={"company_name": "X", "city": "Utrecht"},
            website_intelligence={"total_score": 30},
            anthropic_client=client,
        )
        assert not result["body"].lower().startswith("ik")

    @pytest.mark.asyncio
    async def test_claude_exception_returns_error_no_raise(self):
        client = MagicMock()
        client.messages = MagicMock()
        client.messages.create = AsyncMock(side_effect=RuntimeError("boom"))

        result = await generate_review_email(
            lead={"company_name": "X", "city": "Utrecht"},
            website_intelligence={"total_score": 30},
            anthropic_client=client,
        )
        assert "claude_failed" in result["error"]
        assert result["body"] == ""

    @pytest.mark.asyncio
    async def test_invalid_json_returns_parse_error(self):
        resp = MagicMock()
        content_block = MagicMock()
        content_block.text = "not valid json at all"
        resp.content = [content_block]
        resp.usage = MagicMock(input_tokens=10, output_tokens=5)
        client = _make_anthropic_client(resp)

        result = await generate_review_email(
            lead={"company_name": "X", "city": "Utrecht"},
            website_intelligence={"total_score": 30},
            anthropic_client=client,
        )
        assert "parse_failed" in result["error"]

    @pytest.mark.asyncio
    async def test_cost_log_insert_when_supabase_injected(self):
        resp = _fake_claude_response(subject="X", body="Opvallend punt. Herkenbaar?")
        client = _make_anthropic_client(resp)

        db = MagicMock()
        table = MagicMock()
        table.insert.return_value = table
        table.execute.return_value = MagicMock(data=[])
        db.table.return_value = table

        result = await generate_review_email(
            lead={"company_name": "X", "city": "Utrecht", "id": "lead-1", "workspace_id": "aerys"},
            website_intelligence={"total_score": 30},
            anthropic_client=client,
            supabase_client=db,
        )
        assert "error" not in result
        assert db.table.called
        # insert was invoked on api_cost_log
        call_args = [c.args for c in db.table.call_args_list]
        assert any("api_cost_log" in str(a) for a in call_args)
