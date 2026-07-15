"""tests/test_text_normalizer.py — P1/C2 opener/tekst-normalisatie."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.text_normalizer import normalize_generated_text as N

def test_strips_markdown_header_label():
    raw = "# Openingszin voor Max\n\nMax de Makelaer valt op met 47 reviews."
    out, ok, reason = N(raw)
    assert ok and reason == "ok"
    assert out.startswith("Max de Makelaer")
    assert "#" not in out and "Openingszin" not in out

def test_strips_bold_aanhef_preamble():
    raw = "**Beste Arend Pieter,**\n\nUw bedrijf onderscheidt zich."
    out, ok, _ = N(raw)
    assert ok
    assert out.startswith("Uw bedrijf")
    assert "**" not in out and "Beste" not in out

def test_strips_label_only_line():
    raw = "# Openingszin:\n\nJans Aannemersbedrijf staat bekend om vakwerk."
    out, ok, _ = N(raw)
    assert ok and out.startswith("Jans Aannemersbedrijf")

def test_strips_bold_summary_header():
    raw = "# Max de Makelaer Amsterdam\n\nMakelaarskantoor gespecialiseerd in verkoop."
    out, ok, _ = N(raw)
    assert ok  # header verwijderd, prozatekst blijft
    assert out.startswith("Makelaarskantoor gespecialiseerd")
    assert "# Max" not in out

def test_refusal_rejected():
    out, ok, reason = N("Ik kan hierbij niet helpen omdat er te weinig info is.")
    assert ok is False and reason == "refusal" and out == ""

def test_empty_rejected():
    assert N("")[1] is False
    assert N("   ")[1] is False

def test_code_fence_stripped():
    raw = "```\nEen nette openingszin hier.\n```"
    out, ok, _ = N(raw)
    assert ok and out == "Een nette openingszin hier."

def test_max_sentences_cap():
    raw = "Zin een. Zin twee. Zin drie. Zin vier."
    out, ok, _ = N(raw, max_sentences=2)
    assert ok and out == "Zin een. Zin twee."

def test_clean_text_passes_through():
    raw = "Op jullie site zag ik geen online afspraakoptie. Bewust?"
    out, ok, reason = N(raw)
    assert ok and out == raw and reason == "ok"

def test_empty_after_clean_rejected():
    # alleen een label → na schoonmaak niets over
    out, ok, reason = N("# Openingszin:")
    assert ok is False and reason == "empty_after_clean"


def test_bold_wrapped_preamble_dropped():
    # '**Mogelijke openingszin:**' — preamble in bold; moet gedropt worden.
    raw = '**Mogelijke openingszin:**\n\n"De consistente 4.8-waardering spreekt voor zich."'
    out, ok, _ = N(raw, max_sentences=3)
    assert ok
    assert "openingszin" not in out.lower()
    assert not out.startswith(("*", "#", '"'))
    assert out.startswith("De consistente")


def test_conversational_preamble_with_hr_and_aanhef():
    # 'Hier is een openingszin voor je email:' + '---' + 'Beste X,' allemaal weg.
    raw = "Hier is een openingszin voor je email:\n\n---\n\nBeste Arina,\n\nHet valt op dat jullie site geen chatbot heeft."
    out, ok, _ = N(raw, max_sentences=3)
    assert ok
    assert out == "Het valt op dat jullie site geen chatbot heeft."


def test_orphan_bold_marker_stripped():
    # Onafgesloten '**' aan het begin van de echte zin.
    raw = "**Met 50 reviews op 4.9 heeft de kliniek zich bewezen in Amsterdam."
    out, ok, _ = N(raw, max_sentences=3)
    assert ok and not out.startswith("*")
    assert out.startswith("Met 50 reviews")
