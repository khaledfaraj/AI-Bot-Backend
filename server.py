"""
server.py — AI Bot Platform Backend
====================================
Platforms:
  • Telegram  — Telethon (unchanged)
  • WhatsApp  — neonize (WhatsApp Web Automation)  ← replaces Meta Cloud API

WhatsApp Architecture:
  ┌─ POST /api/whatsapp/connect  ──────────────────────────────────────┐
  │  • Creates a neonize NewClient per user_id                         │
  │  • Runs client.connect() in a background thread                    │
  │  • QR arrives via @client.qr callback → broadcast to SSE queue    │
  └────────────────────────────────────────────────────────────────────┘
  ┌─ GET  /api/whatsapp/qr-stream?userId=…  (SSE) ─────────────────────┐
  │  • Streams QR Base64 PNG to the browser in real-time               │
  │  • Sends "connected" event after successful scan                   │
  └────────────────────────────────────────────────────────────────────┘

Session persistence (Supabase):
  • On successful scan → sqlite file dumped as binary → saved in
    telegram_bot.'Send Code' column (bytea/base64) for the user's row.
  • On bot start → binary loaded → written to a temp file → neonize
    reads it → no QR scan needed again.
"""

from fastapi import FastAPI, HTTPException, Request, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient
from telethon.sessions import StringSession
from supabase import create_client, Client
from dotenv import load_dotenv

import os
import asyncio
import base64
import io
import threading
import tempfile
import shutil
import qrcode
import httpx
import re
from datetime import datetime
from typing import AsyncGenerator

# neonize
from neonize import NewClient
from neonize.events import ConnectedEv, MessageEv
from neonize.proto.Neonize_pb2 import JID

# The Telegram and WhatsApp bot managers — previously run as separate
# standalone processes/services. Importing them here (rather than running
# `python auto_reply_bot.py` / `python whatsapp_bot.py` separately) is what
# lets all three live inside this one Render service. Importing them just
# defines their functions/clients — it does NOT start anything by itself;
# both are explicitly launched from this file's startup() event below.
import auto_reply_bot
import whatsapp_bot

load_dotenv()

app = FastAPI()

# ⚠️ Set ALLOWED_ORIGINS in .env to your real frontend domain(s), comma-separated.
# Example: ALLOWED_ORIGINS=https://yourdomain.com,https://www.yourdomain.com
_allowed_origins = [o.strip() for o in os.getenv('ALLOWED_ORIGINS', '').split(',') if o.strip()]
if not _allowed_origins:
    print("⚠️  ALLOWED_ORIGINS is not set in .env — CORS will block all browser requests until you set it.")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')

print(f"\n🔍 Supabase URL: {supabase_url[:50]}...")
print(f"🔍 Supabase Key: {supabase_key[:20]}...")

try:
    supabase: Client = create_client(supabase_url, supabase_key)
    print("✅ Supabase Connected Successfully\n")
except Exception as e:
    print(f"❌ Supabase Connection Failed: {e}\n")
    supabase = None

# ─────────────────────────────────────────────────────────────────────────────
# 🔐 Session Encryption — shared with auto_reply_bot.py and whatsapp_bot.py
# ─────────────────────────────────────────────────────────────────────────────
# See session_crypto.py for setup instructions (SESSION_ENCRYPTION_KEY in .env).
from cryptography.fernet import InvalidToken
from session_crypto import encrypt_text, decrypt_text, encrypt_bytes, decrypt_bytes, decrypt_bytes_safe

# Active Telegram sessions (unchanged)
active_sessions = {}

# ─────────────────────────────────────────────────────────────────────────────
# WhatsApp / neonize — In-Memory State
# ─────────────────────────────────────────────────────────────────────────────

# { user_id: NewClient }  — one neonize client per user
wa_clients: dict[str, NewClient] = {}

# { user_id: [asyncio.Queue, ...] }  — SSE queues per user
wa_sse_queues: dict[str, list[asyncio.Queue]] = {}
wa_sse_lock = threading.Lock()

# The main FastAPI event loop (set at startup for thread→async bridging)
_main_loop: asyncio.AbstractEventLoop | None = None


@app.on_event("startup")
async def startup():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    print("✅ FastAPI startup — main event loop captured.")

    # ── Launch the Telegram bot manager ─────────────────────────────────
    # run_all_bots() is a long-running async function (its own realtime
    # listener + polling-loop safety net + per-bot Telethon clients) — it
    # shares this same asyncio event loop as the FastAPI app itself, which
    # is exactly what lets the API keep answering HTTP requests at the same
    # time Telegram messages are being processed, with no manual threading
    # needed on this side.
    asyncio.create_task(auto_reply_bot.run_all_bots())
    print("✅ Telegram bot manager started (asyncio task on the main loop).")

    # ── Launch the WhatsApp bot manager ─────────────────────────────────
    # run_all_whatsapp_bots() is a SYNCHRONOUS function that blocks forever
    # (it manages its own neonize client threads internally) — it cannot
    # share this event loop the way the Telegram manager does, so it runs
    # in its own background thread instead. This still achieves the same
    # goal (all three subsystems running concurrently, none blocking the
    # others) — it just uses a thread instead of a task for this one piece,
    # because that's what its underlying library (neonize) requires.
    threading.Thread(
        target=whatsapp_bot.run_all_whatsapp_bots,
        daemon=True,
        name="wa-manager",
    ).start()
    print("✅ WhatsApp bot manager started (background thread).")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _qr_bytes_to_base64_png(qr_data: bytes) -> str:
    """Convert raw QR bytes from neonize into a data:image/png;base64,… string."""
    content = qr_data.decode("utf-8") if isinstance(qr_data, bytes) else qr_data
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(content)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")
    return f"data:image/png;base64,{b64}"


def _broadcast_to_user(user_id: str, payload: str) -> None:
    """Thread-safe push of a payload string into all SSE queues for a user."""
    if _main_loop is None:
        return
    with wa_sse_lock:
        queues = list(wa_sse_queues.get(user_id, []))
    for q in queues:
        asyncio.run_coroutine_threadsafe(q.put(payload), _main_loop)


def _save_session_to_supabase(user_id: str, db_path: str, bot_row_id: str) -> None:
    """
    Reads the neonize sqlite file, base64-encodes it, and saves it into the
    'Send Code' column of the bot row in Supabase.

    Why base64? Supabase JS client stores binary in text columns as base64;
    this keeps it portable without needing a bytea column.
    """
    if not supabase:
        return
    try:
        with open(db_path, "rb") as f:
            raw = f.read()
        encrypted = encrypt_bytes(raw)
        encoded = base64.b64encode(encrypted).decode("utf-8")
        supabase.table('telegram_bot').update({
            'Send Code':  encoded,
            'status':     'active',
            'updated_at': datetime.now().isoformat()
        }).eq('id', bot_row_id).execute()
        print(f"✅ [WA] Session encrypted & saved to Supabase for user {user_id} (bot row {bot_row_id})")
    except Exception as e:
        print(f"❌ [WA] Failed to save session: {e}")


def _load_telegram_session(bot_row_id: str) -> str | None:
    """
    Loads the encrypted Telegram StringSession for a bot row from Supabase
    and decrypts it. Returns None if there's no session stored.

    Use this (instead of reading 'Send Code' directly) anywhere that starts
    a TelegramClient from a saved session — e.g. auto_reply_bot.py.
    """
    if not supabase:
        return None
    try:
        result = (
            supabase.table('telegram_bot')
            .select('"Send Code"')
            .eq('id', bot_row_id)
            .single()
            .execute()
        )
        encrypted = result.data.get('Send Code', '') if result.data else ''
        if not encrypted:
            return None
        try:
            return decrypt_text(encrypted)
        except InvalidToken:
            # Saved before encryption was added — treat as legacy plaintext.
            print(f"⚠️  [TG] Legacy unencrypted session for bot {bot_row_id} — will be re-encrypted on next login.")
            return encrypted
    except Exception as e:
        print(f"⚠️  [TG] Could not load session: {e}")
        return None


def _load_session_from_supabase(bot_row_id: str, tmp_dir: str) -> str | None:
    """
    Loads the base64-encoded sqlite from Supabase, decodes it, writes it to a
    temp file inside tmp_dir, and returns the file path.
    Returns None if no session is stored.
    """
    if not supabase:
        return None
    try:
        result = (
            supabase.table('telegram_bot')
            .select('"Send Code"')
            .eq('id', bot_row_id)
            .single()
            .execute()
        )
        encoded = result.data.get('Send Code', '') if result.data else ''
        if not encoded:
            return None
        raw_decoded = base64.b64decode(encoded)

        try:
            raw = decrypt_bytes(raw_decoded)
        except InvalidToken:
            # Session saved before encryption was added — use it as-is this
            # once. It will be re-saved encrypted the next time this user
            # connects (see _save_session_to_supabase).
            print(f"⚠️  [WA] Legacy unencrypted session for bot {bot_row_id} — will be re-encrypted on next save.")
            raw = raw_decoded

        db_path = os.path.join(tmp_dir, "whatsapp_session.sqlite3")
        with open(db_path, "wb") as f:
            f.write(raw)
        print(f"✅ [WA] Session restored from Supabase → {db_path}")
        return db_path
    except Exception as e:
        print(f"⚠️  [WA] Could not load session: {e}")
        return None


def _extract_text(msg: MessageEv) -> str:
    """Extract plain text from a neonize MessageEv (handles regular + extended)."""
    try:
        if msg.Message.conversation:
            return msg.Message.conversation
        if msg.Message.extendedTextMessage.text:
            return msg.Message.extendedTextMessage.text
    except Exception:
        pass
    return ""


def _build_reply_jid(msg: MessageEv) -> JID | None:
    """
    Build the JID to reply to.
    For group messages → reply to the group chat JID.
    For private messages → reply to the sender JID.
    """
    try:
        src = msg.Info.MessageSource
        if src.IsGroup:
            return src.Chat       # group JID
        return src.Sender         # private chat JID
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# format_numbot_message (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────

def format_numbot_message(data: dict) -> str:
    lines = []
    if data.get('numbotWelcome'):
        lines.append(data['numbotWelcome'])
        lines.append("")
        lines.append("=" * 50)
        lines.append("")
    if data.get('menuStructure'):
        lines.append("📱 Main Menu:")
        lines.append("")
        menu = data['menuStructure']
        for key in sorted(menu.keys(), key=int):
            lines.append(f"{key}️⃣ {menu[key]}")
        lines.append("")
        lines.append("=" * 50)
        lines.append("")
    if data.get('menuStructure', {}).get('1'):
        lines.append(f"1️⃣➕ {data['menuStructure']['1']}:")
        lines.append("")
        for p in data.get('products', []):
            n, d = p.get('name', ''), p.get('description', '')
            lines.append(f"• {n} - {d}" if d else f"• {n}")
        if not data.get('products'):
            lines.append("• No items added yet")
        lines.append("")
        lines.append("📋 Options:")
        lines.append(f"1 → {data['menuStructure'].get('2', 'Next')}")
        lines.append(f"2 → {data['menuStructure'].get('3', 'Next')}")
        lines.append(f"3 → {data['menuStructure'].get('4', 'Next')}")
        lines.append("0 → Back to Main Menu")
        lines.append("")
        lines.append("=" * 50)
        lines.append("")
    if data.get('menuStructure', {}).get('2'):
        lines.append(f"2️⃣➕ {data['menuStructure']['2']}:")
        lines.append("")
        for p in data.get('products2', []):
            n, pr = p.get('name', ''), p.get('price', '')
            lines.append(f"• {n} - {pr}" if pr else f"• {n}")
        if not data.get('products2'):
            lines.append("• No items added yet")
        lines.append("")
        lines.append("📋 Options:")
        lines.append(f"1 → {data['menuStructure'].get('3', 'Next')}")
        lines.append(f"2 → {data['menuStructure'].get('4', 'Next')}")
        lines.append("0 → Back to Main Menu")
        lines.append("")
        lines.append("=" * 50)
        lines.append("")
    if data.get('menuStructure', {}).get('3'):
        lines.append(f"3️⃣➕ {data['menuStructure']['3']}")
        for p in data.get('products3', []):
            n, d = p.get('name', ''), p.get('description', '')
            lines.append(f"• {n} - {d}" if d else f"• {n}")
        if not data.get('products3'):
            lines.append("• No booking options added yet")
        lines.append("")
        lines.append("=" * 50)
        lines.append("")
    if data.get('menuStructure', {}).get('4'):
        lines.append(f"4️⃣➕ {data['menuStructure']['4']}:")
        lines.append("")
        lines.append(data.get('waitingMessage') or "🕐 Please wait, someone is going to contact you soon...")
        lines.append("")
        lines.append("📋 Options:")
        lines.append("0 → Back to Main Menu")
        lines.append("")
        lines.append("=" * 50)
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 🔐 Auth — issues real, signed session tokens
# ─────────────────────────────────────────────────────────────────────────────
# Password checking itself still happens in Postgres (login_user/signup_user,
# salted-hash comparison — see rate_limiting.sql). These endpoints just call
# those same RPC functions from the *server* (using the service_role client,
# same as before) and, on success, hand back a signed token the frontend can
# use to prove who it is on every later request — instead of the frontend
# just asserting a user_id and being trusted.
from auth_tokens import issue_token, verify_token


@app.post("/api/auth/signup")
async def api_signup(request: Request):
    try:
        body         = await request.json()
        username     = (body.get('username') or '').strip()
        password     = (body.get('password') or '').strip()
        phone_number = (body.get('phone_number') or '').strip()

        if not username or not password or not phone_number:
            raise HTTPException(status_code=400, detail="username, password, and phone_number are required")
        if not supabase:
            raise HTTPException(status_code=500, detail="Database not connected")

        result = supabase.rpc('signup_user', {
            'p_username': username,
            'p_password': password,
            'p_phone_number': phone_number
        }).execute()

        row = result.data[0] if result.data else None
        if not row:
            raise HTTPException(status_code=500, detail="Signup failed")

        token = issue_token(row['id'], row['username'])
        print(f"✅ [SIGNUP] New user: {username}")
        return {'success': True, 'token': token, 'user': row}

    except HTTPException:
        raise
    except Exception as e:
        msg = str(e)
        if 'USERNAME_TAKEN' in msg:
            raise HTTPException(status_code=409, detail="Username already taken")
        if 'RATE_LIMITED' in msg:
            raise HTTPException(status_code=429, detail="Too many accounts created recently. Please try again later.")
        print(f"❌ [SIGNUP ERROR]: {e}")
        raise HTTPException(status_code=500, detail="Sign up failed")


@app.post("/api/auth/login")
async def api_login(request: Request):
    try:
        body     = await request.json()
        username = (body.get('username') or '').strip()
        password = (body.get('password') or '').strip()

        if not username or not password:
            raise HTTPException(status_code=400, detail="username and password are required")
        if not supabase:
            raise HTTPException(status_code=500, detail="Database not connected")

        result = supabase.rpc('login_user', {
            'p_username': username,
            'p_password': password
        }).execute()

        row = result.data[0] if result.data else None
        if not row:
            raise HTTPException(status_code=401, detail="Invalid username or password")

        token = issue_token(row['id'], row['username'])
        print(f"✅ [LOGIN] User logged in: {username}")
        return {'success': True, 'token': token, 'user': row}

    except HTTPException:
        raise
    except Exception as e:
        msg = str(e)
        if 'LOCKED' in msg:
            secs = ''.join(ch for ch in msg.split('LOCKED:')[-1] if ch.isdigit()) or '60'
            raise HTTPException(status_code=429, detail=f"Too many failed attempts. Please wait {secs} seconds and try again.")
        if 'INVALID_CREDENTIALS' in msg:
            raise HTTPException(status_code=401, detail="Invalid username or password")
        print(f"❌ [LOGIN ERROR]: {e}")
        raise HTTPException(status_code=500, detail="Login failed")


def get_current_user(request: Request) -> dict:
    """FastAPI dependency: verifies the Authorization: Bearer <token> header
    and returns {'id', 'username'} for the real, verified caller. Use this
    instead of ever trusting a user_id sent in a request body/query."""
    auth_header = request.headers.get('authorization', '')
    if not auth_header.startswith('Bearer '):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    token = auth_header[len('Bearer '):].strip()
    payload = verify_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired session — please log in again")
    return payload


# ─────────────────────────────────────────────────────────────────────────────
# 🗑 Soft Delete Bot — UNCHANGED
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/api/delete-bot")
async def delete_bot(request: Request):
    try:
        body    = await request.json()
        bot_id  = body.get('botId')
        user_id = body.get('userId')

        if not bot_id or not user_id:
            raise HTTPException(status_code=400, detail="botId and userId are required")
        if not supabase:
            raise HTTPException(status_code=500, detail="Database not connected")

        check = (
            supabase.table('telegram_bot')
            .select('id')
            .eq('id', bot_id)
            .eq('user_id', user_id)
            .execute()
        )
        if not check.data:
            raise HTTPException(status_code=404, detail="Bot not found or access denied")

        # Hard delete: remove the row completely from the database
        supabase.table('telegram_bot').delete().eq('id', bot_id).execute()

        # Also tear down any running neonize client for this user
        if user_id in wa_clients:
            try:
                wa_clients[user_id].stop()
            except Exception:
                pass
            del wa_clients[user_id]

        print(f"✅ [DELETE-BOT] Hard-deleted bot {bot_id} for user {user_id}")
        return {'success': True, 'message': 'Bot deleted successfully'}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [DELETE-BOT ERROR]: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# 🟢 WhatsApp — neonize (WhatsApp Web Automation)
# ─────────────────────────────────────────────────────────────────────────────

def _create_neonize_client(user_id: str, bot_row_id: str,
                            welcome_message: str, tmp_dir: str) -> NewClient:
    """
    Build a neonize NewClient wired up with QR, Connected, and Message
    event handlers. The session file lives in tmp_dir/whatsapp_session.sqlite3.
    """
    db_path = os.path.join(tmp_dir, "whatsapp_session.sqlite3")
    session_name = db_path  # neonize accepts a file path as session name

    client = NewClient(session_name)

    # ── QR handler ──────────────────────────────────────────────────────────
    @client.qr
    def on_qr(_c: NewClient, qr_data: bytes) -> None:
        print(f"📲 [WA] QR generated for user {user_id}")
        try:
            b64_png = _qr_bytes_to_base64_png(qr_data)
            _broadcast_to_user(user_id, f"QR:{b64_png}")
        except Exception as e:
            print(f"❌ [WA] QR conversion error: {e}")

    # ── Connected handler ────────────────────────────────────────────────────
    @client.event(ConnectedEv)
    def on_connected(_c: NewClient, _ev: ConnectedEv) -> None:
        print(f"✅ [WA] Connected for user {user_id}! Saving session...")
        _save_session_to_supabase(user_id, db_path, bot_row_id)
        # Save the linked WhatsApp phone number
        try:
            phone_number = _c.get_me().JID.User
            if phone_number and supabase:
                supabase.table('telegram_bot').update({
                    'Phone Number': f'+{phone_number}',
                    'updated_at':   datetime.now().isoformat()
                }).eq('id', bot_row_id).execute()
                print(f"✅ [WA] Phone number saved: +{phone_number}")
        except Exception as e:
            print(f"⚠️  [WA] Could not save phone number: {e}")
        _broadcast_to_user(user_id, "CONNECTED")

    # ── Message handler ──────────────────────────────────────────────────────
    @client.event(MessageEv)
    def on_message(_c: NewClient, msg: MessageEv) -> None:
        try:
            src = msg.Info.MessageSource

            # Only handle private (non-group) messages
            if src.IsGroup:
                return
            # Ignore own messages
            if src.IsFromMe:
                return

            text = _extract_text(msg)
            sender_user = src.Sender.User
            print(f"💬 [WA] Message from {sender_user}: {text!r}")

            if not text:
                return

            # Reply with the bot's welcome message
            reply_jid = _build_reply_jid(msg)
            if reply_jid and welcome_message:
                client.send_message(reply_jid, welcome_message)
                print(f"✅ [WA] Replied to {sender_user}")

        except Exception as e:
            print(f"❌ [WA] Message handler error: {e}")

    return client


def _run_neonize(client: NewClient) -> None:
    """Blocking neonize connect — runs in a daemon thread."""
    try:
        client.connect()
    except Exception as e:
        print(f"❌ [WA] neonize thread error: {e}")


@app.post("/api/whatsapp/connect")
async def whatsapp_connect(request: Request):
    """
    Initialise a neonize WhatsApp client for the user.

    Body (JSON):
      userId, botName, botType, welcomeMessage,
      menuStructure, products, products2, products3, waitingMessage

    Flow:
      1. Create a row in telegram_bot (status='pending')
      2. Try to restore a saved session from Supabase
      3. If no session → neonize generates QR → SSE streams it to browser
      4. After scan → session saved back to Supabase automatically
    """
    try:
        body            = await request.json()
        user_id         = body.get('userId', '').strip()
        bot_name        = body.get('botName', 'MyWhatsAppBot')
        bot_type        = body.get('botType', 'welcome_bot')
        welcome_message = body.get('welcomeMessage', '')
        menu_structure  = body.get('menuStructure', {})
        products        = body.get('products', [])
        products2       = body.get('products2', [])
        products3       = body.get('products3', [])
        waiting_message = body.get('waitingMessage', '')

        if not user_id:
            raise HTTPException(status_code=400, detail="userId is required")

        print(f"\n🟢 [WA] Connecting WhatsApp for user: {user_id}")

        # Format welcome message
        if bot_type == 'number_bot':
            formatted_message = format_numbot_message({
                'numbotWelcome': welcome_message, 'menuStructure': menu_structure,
                'products': products, 'products2': products2,
                'products3': products3, 'waitingMessage': waiting_message
            })
        else:
            formatted_message = welcome_message

        if not supabase:
            raise HTTPException(status_code=500, detail="Database not connected")

        # ── Step 1: Insert bot row (or reuse existing pending row) ───────────
        existing = (
            supabase.table('telegram_bot')
            .select('id, "Send Code"')
            .eq('user_id', user_id)
            .eq('platform', 'whatsapp')
            .neq('bot_name', None)      # skip soft-deleted rows
            .limit(1)
            .execute()
        )

        if existing.data:
            bot_row_id   = existing.data[0]['id']
            saved_session = existing.data[0].get('Send Code', '')
            print(f"[WA] Reusing existing bot row {bot_row_id}")
        else:
            insert_result = supabase.table('telegram_bot').insert([{
                'user_id':         user_id,
                'bot_name':        bot_name,
                'bot_type':        bot_type,
                'platform':        'whatsapp',
                'welcome_message': formatted_message,
                'Phone Number':    '',
                'API ID':          '',        # not used for neonize
                'API Hash':        '',        # not used for neonize
                'Send Code':       '',        # will hold the session binary
                'status':          'pending',
                'created_at':      datetime.now().isoformat(),
                'updated_at':      datetime.now().isoformat()
            }]).execute()
            bot_row_id   = insert_result.data[0]['id']
            saved_session = ''
            print(f"[WA] Created new bot row {bot_row_id}")

        # Update welcome message in case it changed
        supabase.table('telegram_bot').update({
            'welcome_message': formatted_message,
            'updated_at':      datetime.now().isoformat()
        }).eq('id', bot_row_id).execute()

        # ── Step 2: Stop previous client for this user (if any) ─────────────
        if user_id in wa_clients:
            try:
                wa_clients[user_id].stop()
            except Exception:
                pass
            del wa_clients[user_id]

        # ── Step 3: Temp dir for session file ───────────────────────────────
        tmp_dir = tempfile.mkdtemp(prefix=f"wa_session_{user_id}_")

        # ── Step 4: Try restoring saved session ─────────────────────────────
        if saved_session:
            _load_session_from_supabase(bot_row_id, tmp_dir)
            # neonize will auto-detect the sqlite file and skip QR if valid

        # ── Step 5: Build and start neonize client ───────────────────────────
        client = _create_neonize_client(user_id, bot_row_id, formatted_message, tmp_dir)
        wa_clients[user_id] = client

        thread = threading.Thread(
            target=_run_neonize,
            args=(client,),
            daemon=True,
            name=f"wa-{user_id}"
        )
        thread.start()
        print(f"🚀 [WA] neonize thread started for user {user_id}")

        return {
            "success":  True,
            "botRowId": bot_row_id,
            "message":  "WhatsApp client started. Connect to /api/whatsapp/qr-stream to get the QR code.",
            "qrStream": f"/api/whatsapp/qr-stream?userId={user_id}"
        }

    except HTTPException:
        raise
    except Exception as error:
        print(f"❌ [WA CONNECT ERROR]: {error}")
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/api/whatsapp/qr-stream")
async def whatsapp_qr_stream(userId: str = Query(...)):
    """
    SSE endpoint — streams events to the browser for a specific user.

    Event types sent:
      • event: status   data: waiting          ← first ping (connection OK)
      • event: qr       data: QR:<base64png>   ← new QR code ready to scan
      • event: connected data: success         ← scan successful
      • event: heartbeat data: ping            ← keepalive every 20 s
    """
    if not userId:
        raise HTTPException(status_code=400, detail="userId is required")

    queue: asyncio.Queue = asyncio.Queue()

    with wa_sse_lock:
        if userId not in wa_sse_queues:
            wa_sse_queues[userId] = []
        wa_sse_queues[userId].append(queue)

    print(f"📡 [WA SSE] Client connected for user {userId}")

    async def event_generator() -> AsyncGenerator[str, None]:
        try:
            yield "event: status\ndata: waiting\n\n"

            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=20.0)

                    if payload == "CONNECTED":
                        yield "event: connected\ndata: success\n\n"
                        print(f"📡 [WA SSE] Sent 'connected' to user {userId}")
                        break

                    elif payload.startswith("QR:"):
                        b64_png = payload[3:]  # strip "QR:" prefix
                        yield f"event: qr\ndata: {b64_png}\n\n"
                        print(f"📡 [WA SSE] Sent QR image to user {userId}")

                except asyncio.TimeoutError:
                    yield "event: heartbeat\ndata: ping\n\n"

        except asyncio.CancelledError:
            print(f"📡 [WA SSE] Client disconnected: {userId}")
        finally:
            with wa_sse_lock:
                if userId in wa_sse_queues and queue in wa_sse_queues[userId]:
                    wa_sse_queues[userId].remove(queue)
                    if not wa_sse_queues[userId]:
                        del wa_sse_queues[userId]

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/whatsapp/status")
async def whatsapp_status(userId: str = Query(...)):
    """Returns the current connection status for a user's WhatsApp bot."""
    client = wa_clients.get(userId)
    if client is None:
        return {"connected": False, "status": "not_initialized"}
    try:
        connected = client.is_connected()
        logged_in = client.is_logged_in()
        return {
            "connected": connected,
            "logged_in": logged_in,
            "status":    "active" if (connected and logged_in) else "connecting"
        }
    except Exception as e:
        return {"connected": False, "status": "error", "detail": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# 🔵 Telegram Section — UNCHANGED
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/api/send-code")
async def send_code(request: Request):
    try:
        body = await request.json()
        print("\n" + "="*60)
        print("📩 [TELEGRAM] RECEIVED REQUEST:")
        print("="*60)
        phone_number    = body.get('phoneNumber')
        api_id          = body.get('apiId')
        api_hash        = body.get('apiHash')
        user_id         = body.get('userId')
        bot_name        = body.get('botName', 'MyBot')
        bot_type        = body.get('botType', 'welcome_bot')
        welcome_message = body.get('welcomeMessage', '')
        menu_structure  = body.get('menuStructure', {})
        products        = body.get('products', [])
        products2       = body.get('products2', [])
        products3       = body.get('products3', [])
        waiting_message = body.get('waitingMessage', '')

        if not phone_number or not api_id or not api_hash:
            raise HTTPException(status_code=400, detail="Missing required fields")
        try:
            api_id = int(api_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="API ID must be a number")

        if bot_type == 'number_bot':
            formatted_message = format_numbot_message({
                'numbotWelcome': welcome_message, 'menuStructure': menu_structure,
                'products': products, 'products2': products2,
                'products3': products3, 'waitingMessage': waiting_message
            })
        else:
            formatted_message = welcome_message

        session = StringSession('')
        tg_client = TelegramClient(session, api_id, api_hash)
        print("🔄 Connecting to Telegram...")
        await tg_client.connect()
        print("📨 Sending verification code...")
        result = await tg_client.send_code_request(phone_number)
        session_id = f'session_{user_id}_{datetime.now().timestamp()}'
        active_sessions[session_id] = {
            'client': tg_client, 'phone_number': phone_number,
            'api_id': api_id, 'api_hash': api_hash,
            'phone_code_hash': result.phone_code_hash,
            'user_id': user_id, 'bot_name': bot_name,
            'bot_type': bot_type, 'formatted_message': formatted_message
        }
        if supabase:
            try:
                existing = (
                    supabase.table('telegram_bot')
                    .select('id')
                    .eq('user_id', user_id)
                    .eq('platform', 'telegram')
                    .eq('Phone Number', phone_number)
                    .limit(1)
                    .execute()
                )
                if existing.data:
                    supabase.table('telegram_bot').update({
                        'bot_name':        bot_name,
                        'bot_type':        bot_type,
                        'welcome_message': formatted_message,
                        'API ID':          api_id,
                        'API Hash':        api_hash,
                        'Send Code':       '',
                        'status':          'pending',
                        'updated_at':      datetime.now().isoformat()
                    }).eq('id', existing.data[0]['id']).execute()
                    print("✅ [TELEGRAM] Updated existing Supabase row\n")
                else:
                    supabase.table('telegram_bot').insert([{
                        'user_id':         user_id,
                        'bot_name':        bot_name,
                        'bot_type':        bot_type,
                        'platform':        'telegram',
                        'welcome_message': formatted_message,
                        'Phone Number':    phone_number,
                        'API ID':          api_id,
                        'API Hash':        api_hash,
                        'Send Code':       '',
                        'status':          'pending',
                        'created_at':      datetime.now().isoformat(),
                        'updated_at':      datetime.now().isoformat()
                    }]).execute()
                    print("✅ [TELEGRAM] Inserted new Supabase row\n")
            except Exception as db_error:
                print(f"\n❌ DATABASE ERROR: {db_error}")
        return {'success': True, 'sessionId': session_id, 'message': 'Code sent successfully'}
    except Exception as error:
        print(f"\n❌ ERROR: {str(error)}")
        raise HTTPException(status_code=500, detail=str(error))


@app.post("/api/verify-code")
async def verify_code(request: Request):
    try:
        body = await request.json()
        session_id = body.get('sessionId')
        code       = body.get('code')
        user_id    = body.get('userId')

        if not session_id or not code:
            raise HTTPException(status_code=400, detail="Missing sessionId or code")
        session_data = active_sessions.get(session_id)
        if not session_data:
            raise HTTPException(status_code=404, detail="Session not found or expired")

        tg_client       = session_data['client']
        phone_number    = session_data['phone_number']
        phone_code_hash = session_data['phone_code_hash']
        await tg_client.sign_in(phone=phone_number, code=code, phone_code_hash=phone_code_hash)
        session_string = tg_client.session.save()
        encrypted_session = encrypt_text(session_string)

        if supabase:
            try:
                bot_name      = session_data.get('bot_name', 'MyBot')
                bot_type      = session_data.get('bot_type', 'welcome_bot')
                formatted_msg = session_data.get('formatted_message', '')
                supabase.table('telegram_bot').update({
                    'Send Code':       encrypted_session,
                    'bot_name':        bot_name,
                    'bot_type':        bot_type,
                    'welcome_message': formatted_msg,
                    'Phone Number':    phone_number,
                    'API ID':          session_data.get('api_id', ''),
                    'API Hash':        session_data.get('api_hash', ''),
                    'status':          'active',
                    'updated_at':      datetime.now().isoformat()
                }).eq('Phone Number', phone_number).eq('user_id', user_id).execute()
                print(f"✅ [TELEGRAM] Bot activated in Supabase for {phone_number} (session encrypted)\n")
            except Exception as db_error:
                print(f"❌ Database update error: {db_error}\n")

        await tg_client.disconnect()
        del active_sessions[session_id]
        return {'success': True, 'sessionString': session_string}
    except Exception as error:
        print(f"\n❌ ERROR: {str(error)}")
        raise HTTPException(status_code=500, detail=str(error))


@app.post("/api/activate-bot")
async def activate_bot(request: dict):
    try:
        user_id  = request.get('userId')
        bot_name = request.get('botName')
        if not user_id or not bot_name:
            raise HTTPException(status_code=400, detail="Missing userId or botName")
        if supabase:
            supabase.table('telegram_bot').update({
                'status': 'active', 'updated_at': datetime.now().isoformat()
            }).eq('user_id', user_id).eq('bot_name', bot_name).execute()
        return {'success': True, 'message': f'Bot "{bot_name}" is now active!'}
    except Exception as error:
        print(f"❌ Activate error: {error}")
        raise HTTPException(status_code=500, detail=str(error))


# ─────────────────────────────────────────────────────────────────────────────
# 🤖 Bot Management — all protected by get_current_user(), all ownership
# checks happen here (never trust an id sent by the browser alone).
# ─────────────────────────────────────────────────────────────────────────────

def _get_owned_bot(bot_id: str, user_id: str) -> dict:
    """Fetch a bot row and raise 404 unless it really belongs to user_id."""
    if not supabase:
        raise HTTPException(status_code=500, detail="Database not connected")
    result = supabase.table('telegram_bot').select('*').eq('id', bot_id).execute()
    row = result.data[0] if result.data else None
    if not row or row.get('user_id') != user_id:
        # Same response whether it doesn't exist or belongs to someone else —
        # don't leak which one it is.
        raise HTTPException(status_code=404, detail="Bot not found")
    return row


@app.get("/api/bots")
async def list_bots(current_user: dict = Depends(get_current_user)):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database not connected")
    result = (
        supabase.table('telegram_bot')
        .select('*')
        .eq('user_id', current_user['id'])
        .not_.is_('bot_name', 'null')
        .order('created_at', desc=True)
        .execute()
    )
    return {'bots': result.data}


@app.post("/api/bots/{bot_id}/toggle")
async def toggle_bot(bot_id: str, current_user: dict = Depends(get_current_user)):
    bot = _get_owned_bot(bot_id, current_user['id'])
    new_status = 'paused' if bot.get('status') == 'active' else 'active'
    supabase.table('telegram_bot').update({
        'status': new_status,
        'updated_at': datetime.now().isoformat()
    }).eq('id', bot_id).execute()
    return {'success': True, 'status': new_status}


@app.post("/api/bots/{bot_id}/welcome-message")
async def update_welcome_message(bot_id: str, request: Request, current_user: dict = Depends(get_current_user)):
    body = await request.json()
    message = (body.get('welcome_message') or '').strip()
    if not message:
        raise HTTPException(status_code=400, detail="welcome_message is required")
    _get_owned_bot(bot_id, current_user['id'])
    supabase.table('telegram_bot').update({
        'welcome_message': message,
        'updated_at': datetime.now().isoformat()
    }).eq('id', bot_id).execute()
    return {'success': True}


@app.post("/api/bots/activate-pending")
async def activate_pending_bot(request: Request, current_user: dict = Depends(get_current_user)):
    """
    Used by create-bot.html's final step. The frontend doesn't know the
    row's id at this point (it was created server-side during send-code/
    verify-code or whatsapp/connect) — so instead of trusting a client-
    supplied user_id + row lookup, we find the row using the VERIFIED
    user id from the token, scoped to the right platform/status.
    """
    if not supabase:
        raise HTTPException(status_code=500, detail="Database not connected")

    body     = await request.json()
    platform = (body.get('platform') or '').strip().lower()
    bot_name = (body.get('bot_name') or '').strip()

    if platform not in ('telegram', 'whatsapp'):
        raise HTTPException(status_code=400, detail="platform must be 'telegram' or 'whatsapp'")
    if not bot_name:
        raise HTTPException(status_code=400, detail="bot_name is required")

    query = supabase.table('telegram_bot').select('id').eq('user_id', current_user['id']).eq('platform', platform)
    query = query.eq('status', 'pending') if platform == 'telegram' else query.eq('status', 'active')
    result = query.order('created_at', desc=True).limit(1).execute()

    row = result.data[0] if result.data else None
    if not row:
        raise HTTPException(status_code=404, detail="No pending bot found to activate")

    supabase.table('telegram_bot').update({
        'status':     'active',
        'bot_name':   bot_name,
        'updated_at': datetime.now().isoformat()
    }).eq('id', row['id']).execute()

    return {'success': True}


@app.post("/api/bots/{bot_id}/activate")
async def activate_bot(bot_id: str, request: Request, current_user: dict = Depends(get_current_user)):
    """Generic version of the above for when the frontend DOES know the bot id."""
    body = await request.json()
    bot_name = (body.get('bot_name') or '').strip()
    _get_owned_bot(bot_id, current_user['id'])
    update_data = {'status': 'active', 'updated_at': datetime.now().isoformat()}
    if bot_name:
        update_data['bot_name'] = bot_name
    supabase.table('telegram_bot').update(update_data).eq('id', bot_id).execute()
    return {'success': True}


@app.delete("/api/bots/{bot_id}")
async def delete_bot(bot_id: str, current_user: dict = Depends(get_current_user)):
    _get_owned_bot(bot_id, current_user['id'])
    supabase.table('telegram_bot').delete().eq('id', bot_id).execute()
    return {'success': True}


# ─────────────────────────────────────────────────────────────────────────────
# Root
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "status":                    "running",
        "message":                   "AI Bot Backend — API + Telegram + WhatsApp, merged into one service",
        "active_telegram_sessions":  len(active_sessions),
        "active_whatsapp_clients":   len(wa_clients),
        "supabase_connected":        supabase is not None,
        "whatsapp":                  "neonize (WhatsApp Web Automation)",
        "auth":                      "Supabase username/password (no Firebase)"
    }


@app.get("/health")
async def health_check():
    # Simple, fast, dependency-free 200 — this is what Render/UptimeRobot
    # should be pointed at. It does not query Supabase or anything else
    # that could be slow or momentarily down; a basic process-alive check
    # is exactly what a health check is for.
    return {"status": "ok", "message": "Bot is running"}


if __name__ == '__main__':
    import uvicorn
    # Render injects PORT and expects the app to bind to it — 5000 is only
    # the local-dev fallback when that variable isn't set.
    port = int(os.getenv('PORT', 5000))
    print("\n" + "="*60)
    print("🚀 AI BOT BACKEND SERVER (API + Telegram + WhatsApp, one service)")
    print("="*60)
    print(f"📡 Port  : {port}")
    print(f"🔗 URL   : http://localhost:{port}")
    print(f"🟢 WA    : neonize (WhatsApp Web — Free)")
    print(f"💬 TG    : Telegram via Telethon")
    print(f"🔐 Auth  : Supabase username/password")
    print("="*60)
    uvicorn.run(app, host="0.0.0.0", port=port)
