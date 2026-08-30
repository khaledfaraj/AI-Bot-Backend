"""
session_crypto.py — shared session encryption helpers
=======================================================
Used by server.py, auto_reply_bot.py, and whatsapp_bot.py so every part of
the platform encrypts/decrypts Telegram/WhatsApp sessions with the exact
same key and logic. Do not duplicate this logic elsewhere.

The 'Send Code' column stores either a Telegram StringSession or a
WhatsApp (neonize) sqlite session file — both are equivalent to a full
login token for the user's account and must never be stored in plaintext.

Setup:
  1) Generate a key once:
       python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  2) Put it in .env as SESSION_ENCRYPTION_KEY.
  3) Never commit it, never reuse it across environments (dev/prod should
     have different keys). If it's ever lost, every stored session becomes
     unrecoverable and users will need to reconnect/re-scan.
"""

import os
from dotenv import load_dotenv
from cryptography.fernet import Fernet, InvalidToken

load_dotenv()

SESSION_ENCRYPTION_KEY = os.getenv('SESSION_ENCRYPTION_KEY')
if not SESSION_ENCRYPTION_KEY:
    raise RuntimeError(
        "SESSION_ENCRYPTION_KEY is missing from .env. Generate one with:\n"
        '  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"\n'
        "and add it to .env before starting this process."
    )

_fernet = Fernet(SESSION_ENCRYPTION_KEY.encode())


# ── Text (Telegram StringSession) ────────────────────────────────────────

def encrypt_text(plain: str) -> str:
    """Encrypt a string (e.g. a Telegram session string) before storing it."""
    if not plain:
        return ''
    return _fernet.encrypt(plain.encode('utf-8')).decode('utf-8')


def decrypt_text(token: str) -> str:
    """Decrypt a string previously produced by encrypt_text().
    Raises cryptography.fernet.InvalidToken if it's not a valid token
    for this key (e.g. wrong key, corrupted data, or legacy plaintext)."""
    if not token:
        return ''
    return _fernet.decrypt(token.encode('utf-8')).decode('utf-8')


def decrypt_text_safe(token: str, context: str = '') -> str:
    """Like decrypt_text(), but falls back to returning the raw value if
    it turns out to be a legacy (pre-encryption) plaintext session rather
    than raising. Use this everywhere a *bot* loads a stored session so an
    old unencrypted row doesn't break it — it gets re-encrypted on next save."""
    if not token:
        return ''
    try:
        return decrypt_text(token)
    except InvalidToken:
        label = f" for {context}" if context else ""
        print(f"⚠️  Legacy unencrypted session{label} — will be re-encrypted on next save.")
        return token


# ── Bytes (WhatsApp neonize sqlite session file) ─────────────────────────

def encrypt_bytes(raw: bytes) -> bytes:
    """Encrypt raw bytes (e.g. the WhatsApp sqlite session file) before storing."""
    return _fernet.encrypt(raw)


def decrypt_bytes(token: bytes) -> bytes:
    """Decrypt bytes previously produced by encrypt_bytes()."""
    return _fernet.decrypt(token)


def decrypt_bytes_safe(token: bytes, context: str = '') -> bytes:
    """Like decrypt_bytes(), but falls back to the raw bytes if it turns
    out to be a legacy (pre-encryption) plaintext sqlite file."""
    try:
        return decrypt_bytes(token)
    except InvalidToken:
        label = f" for {context}" if context else ""
        print(f"⚠️  Legacy unencrypted session{label} — will be re-encrypted on next save.")
        return token