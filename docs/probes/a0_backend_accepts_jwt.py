"""
A0 sessie-continuïteit — probe voor de backend-kant (Sprint 5).

Bewijst schakel 4 van de keten: Heatr's backend accepteert de gedeelde
Supabase-JWT die de frontend meestuurt. Reproduceert exact de decode uit
api.main._jwt_workspace tegen een Supabase-vormig token, gesigned met het
SUPABASE_JWT_SECRET uit Heatr's .env (hetzelfde project-secret dat Warmr's
login gebruikt).

Draai:  python3 docs/probes/a0_backend_accepts_jwt.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import jwt as _jwt  # PyJWT, zoals api/main.py


def _load_secret() -> str:
    # Lees SUPABASE_JWT_SECRET uit Heatr's .env (waarde nooit printen).
    env = Path(__file__).resolve().parents[2] / ".env"
    for line in env.read_text(encoding="utf-8").splitlines():
        if line.startswith("SUPABASE_JWT_SECRET="):
            return line.split("=", 1)[1].strip()
    return ""


def _jwt_workspace_decode(token: str, secret: str, default_workspace: str) -> str | None:
    """Identiek aan api.main._jwt_workspace (kernlogica)."""
    try:
        payload = _jwt.decode(
            token, secret, algorithms=["HS256"], audience="authenticated",
        )
    except _jwt.InvalidTokenError:
        return None
    app_meta = payload.get("app_metadata") or {}
    return app_meta.get("workspace_id") or default_workspace


def main() -> int:
    secret = _load_secret()
    if not secret:
        print("FAIL  SUPABASE_JWT_SECRET niet gezet in Heatr/.env")
        return 1

    now = int(time.time())
    # Supabase-user-JWT zoals supabase-js na login opslaat (aud=authenticated).
    good = _jwt.encode(
        {"sub": "user-123", "aud": "authenticated", "role": "authenticated",
         "email": "sami@aerys.nl", "iat": now, "exp": now + 3600,
         "app_metadata": {}},
        secret, algorithm="HS256",
    )
    # Zelfde token maar met een verkeerd secret → moet geweigerd worden.
    bad = _jwt.encode(
        {"sub": "x", "aud": "authenticated", "iat": now, "exp": now + 3600},
        "wrong-secret", algorithm="HS256",
    )

    ok = 0
    total = 3

    ws = _jwt_workspace_decode(good, secret, "aerys")
    if ws == "aerys":
        print("PASS  geldige Supabase-JWT → workspace 'aerys' (default, single-tenant)")
        ok += 1
    else:
        print(f"FAIL  geldige JWT gaf {ws!r}, verwacht 'aerys'")

    ws_claim = _jwt.encode(
        {"sub": "u", "aud": "authenticated", "iat": now, "exp": now + 3600,
         "app_metadata": {"workspace_id": "aerys-2"}},
        secret, algorithm="HS256",
    )
    if _jwt_workspace_decode(ws_claim, secret, "aerys") == "aerys-2":
        print("PASS  app_metadata.workspace_id wint van default")
        ok += 1
    else:
        print("FAIL  workspace_id-claim niet gehonoreerd")

    if _jwt_workspace_decode(bad, secret, "aerys") is None:
        print("PASS  JWT met verkeerd secret → geweigerd (None)")
        ok += 1
    else:
        print("FAIL  verkeerd-gesigned token werd geaccepteerd")

    print(f"\n{ok}/{total} geslaagd")
    return 0 if ok == total else 1


if __name__ == "__main__":
    sys.exit(main())
