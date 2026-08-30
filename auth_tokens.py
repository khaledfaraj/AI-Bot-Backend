"""
auth_tokens.py — signed session tokens
========================================
Replaces "trust whatever user_id the browser sends" with a real,
unforgeable session token, issued by the server at login/signup and
verified by the server on every protected request.

The token is a signed (not encrypted) payload: anyone can read it, but
nobody can create or modify one without AUTH_SECRET, which only the
server knows. That's enough here since it only carries a user id/username,
nothing sensitive.

Setup:
  Generate a secret once:
    python -c "import secrets; print(secrets.token_urlsafe(48))"
  Put it in .env as AUTH_SECRET (different from SESSION_ENCRYPTION_KEY —
  don't reuse secrets across purposes).
"""

import os
from dotenv import load_dotenv
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

load_dotenv()

AUTH_SECRET = os.getenv('AUTH_SECRET')
if not AUTH_SECRET:
    raise RuntimeError(
        "AUTH_SECRET is missing from .env. Generate one with:\n"
        '  python -c "import secrets; print(secrets.token_urlsafe(48))"\n'
        "and add it to .env before starting this process."
    )

_serializer = URLSafeTimedSerializer(AUTH_SECRET, salt='ai-bot-session-v1')

# How long an issued token stays valid before the user has to log in again.
TOKEN_MAX_AGE_SECONDS = 60 * 60 * 24 * 7  # 7 days


def issue_token(user_id: str, username: str) -> str:
    """Create a signed session token for a user who just logged in / signed up."""
    return _serializer.dumps({'id': user_id, 'username': username})


def verify_token(token: str) -> dict | None:
    """Verify a token's signature and expiry. Returns {'id', 'username'} or None."""
    if not token:
        return None
    try:
        return _serializer.loads(token, max_age=TOKEN_MAX_AGE_SECONDS)
    except (BadSignature, SignatureExpired):
        return None