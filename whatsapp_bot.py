"""
whatsapp_bot.py — neonize WhatsApp Web Bot
==========================================
Architecture mirrors auto_reply_bot.py (Telegram) exactly:

  * Loads ALL active WhatsApp bots from Supabase at startup
  * One neonize NewClient per bot, each in its own daemon thread
  * A dedicated asyncio event loop per bot for non-blocking async logic
  * All message handling is async — zero blocking of the event loop
  * Strict private-only filter: groups, communities, status, broadcast ignored
  * Supports both bot types: welcome_bot and number_bot (same parser as TG)
  * Sessions loaded from Supabase 'Send Code' column (base64 sqlite3)
  * Programmatic API: run_whatsapp_bot_for_user(user_id)

Run standalone (all active bots):
    python whatsapp_bot.py

Run for one user:
    python whatsapp_bot.py <user_id>
"""

import asyncio
import base64
import logging
import os
import re
import sys
import tempfile
import threading
from datetime import datetime, timezone
from typing import Optional

from neonize import NewClient
from neonize.events import ConnectedEv, MessageEv
from neonize.proto.Neonize_pb2 import JID
from supabase import create_client, Client, acreate_client, AsyncClient
from dotenv import load_dotenv

from session_crypto import encrypt_bytes, decrypt_bytes_safe

load_dotenv()

# Silence noisy library logs — only show ERRORs
logging.getLogger("whatsmeow").setLevel(logging.ERROR)
logging.getLogger("neonize").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("httpcore").setLevel(logging.ERROR)

# NOTE: this file's standalone health-check HTTP server was removed here.
# Now that this module is imported by server.py and run inside the same
# process, server.py's own FastAPI app (and its single port / /health
# route) is what Render pings — a second server trying to bind the same
# $PORT from inside this module would crash with "address already in use".

# ─────────────────────────────────────────────────────────────────────────
# 🕒 Stale-message guard (Flush Pending Updates)
# ─────────────────────────────────────────────────────────────────────────
# Same purpose as the matching guard in auto_reply_bot.py: WhatsApp (via
# whatsmeow) delivers messages received while you were offline the moment
# the connection re-opens — that's inherent to how WhatsApp keeps devices
# in sync and isn't something a client-side flag can turn off. So instead
# of trying to suppress delivery, we check each message's own timestamp
# and skip anything from before this process started / older than
# MAX_MESSAGE_AGE_SECONDS — see _get_message_timestamp() and its use in
# handle_message_async() below.
PROCESS_START_TIME = datetime.now(timezone.utc)
MAX_MESSAGE_AGE_SECONDS = 30

supabase: Client = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

REPLY_DELAY   = 3   # seconds before every auto-reply
POLL_INTERVAL = 300  # seconds — now just a SAFETY NET; Supabase Realtime
                      # (see _start_realtime_listener_async below) is the
                      # primary, fast path for picking up newly-activated bots

# Per-bot menu navigation state: {bot_id: {sender_number: 'main'|'section_1'|...}}
user_states: dict[str, dict[str, str]] = {}

# Bot registry: {bot_id: {'client': NewClient, 'thread': Thread, 'loop': Loop, ...}}
_registry: dict[str, dict] = {}
_registry_lock = threading.Lock()


# =============================================================================
# Session Helpers (Supabase <-> sqlite3 binary)
# =============================================================================

def load_session_from_supabase(bot_row_id: str, tmp_dir: str) -> Optional[str]:
    """
    Reads the base64-encoded sqlite3 session from Supabase 'Send Code' column,
    writes it to a temp file, returns the file path.  Returns None if empty.
    """
    try:
        result = (
            supabase.table('telegram_bot')
            .select('"Send Code"')
            .eq('id', bot_row_id)
            .limit(1)
            .execute()
        )
        encoded = result.data[0].get('Send Code', '') if result.data else ''
        if not encoded:
            return None
        raw_decoded = base64.b64decode(encoded)
        raw = decrypt_bytes_safe(raw_decoded, context=f"bot {bot_row_id}")
        db_path = os.path.join(tmp_dir, "whatsapp_session.sqlite3")
        with open(db_path, "wb") as f:
            f.write(raw)
        print(f"  ✓ Session restored")
        return db_path
    except Exception as e:
        print(f"  ✗ Could not load session: {e}")
        return None


def save_session_to_supabase(db_path: str, bot_row_id: str) -> None:
    """Encrypts the neonize sqlite3 file, base64-encodes it, and saves it to Supabase."""
    try:
        with open(db_path, "rb") as f:
            raw = f.read()
        encrypted = encrypt_bytes(raw)
        encoded = base64.b64encode(encrypted).decode("utf-8")
        supabase.table('telegram_bot').update({
            'Send Code': encoded,
            'status':    'active',
        }).eq('id', bot_row_id).execute()
    except Exception as e:
        print(f"[WA] Failed to save session: {e}")


# =============================================================================
# Message Filtering Helpers
# =============================================================================

_SERVER_PRIVATE    = "s.whatsapp.net"
_SERVER_GROUP      = "g.us"
_SERVER_BROADCAST  = "broadcast"
_SERVER_STATUS     = "status"
_SERVER_NEWSLETTER = "newsletter"


def _is_private_message(msg: MessageEv) -> bool:
    """
    Returns True ONLY for direct 1-to-1 private messages.
    FIX: Chat.Server is EMPTY STRING for private chats in neonize —
    only reject explicitly non-private server values.
    """
    try:
        src = msg.Info.MessageSource
        if src.IsFromMe:  return False
        if src.IsGroup:   return False
        chat_server = src.Chat.Server
        if chat_server in (_SERVER_GROUP, _SERVER_BROADCAST, _SERVER_STATUS, _SERVER_NEWSLETTER):
            return False
        return True
    except Exception:
        return False


def _extract_text(msg: MessageEv) -> str:
    try:
        if msg.Message.conversation:
            return msg.Message.conversation
        if msg.Message.extendedTextMessage.text:
            return msg.Message.extendedTextMessage.text
    except Exception:
        pass
    return ""


def _get_sender_id(msg: MessageEv) -> str:
    try:
        return msg.Info.MessageSource.Sender.User
    except Exception:
        return "unknown"


def _get_reply_jid(msg: MessageEv) -> Optional[JID]:
    try:
        return msg.Info.MessageSource.Sender
    except Exception:
        return None


def _get_message_timestamp(msg: MessageEv):
    """
    Best-effort extraction of msg.Info.Timestamp (when WhatsApp says the
    message was actually sent) as a timezone-aware UTC datetime.

    ⚠️ neonize wraps whatsmeow's Go struct via protobuf/FFI, and its exact
    Python shape for this field isn't pinned down in the docs — this tries
    the shapes protobuf timestamps commonly take (a `google.protobuf.
    Timestamp`-like object, a raw int/float epoch, or an already-built
    datetime) and returns None if nothing matches, rather than guessing
    wrong. handle_message_async() treats None as "can't verify age, don't
    drop it" so a parsing miss fails safe instead of silently eating live
    messages.

    IMPORTANT: check the printed message age in your logs after deploying
    — if you see "could not read message timestamp" instead of an age in
    seconds, the field shape differs from what's handled here and this
    needs a one-line adjustment for your installed neonize version.
    """
    try:
        ts = msg.Info.Timestamp
    except Exception:
        return None

    try:
        if hasattr(ts, 'ToDatetime'):          # protobuf well-known Timestamp
            return ts.ToDatetime().replace(tzinfo=timezone.utc)
        if hasattr(ts, 'seconds'):             # protobuf Timestamp-like (.seconds/.nanos)
            return datetime.fromtimestamp(ts.seconds, tz=timezone.utc)
        if isinstance(ts, (int, float)):       # raw unix epoch — seconds or ms
            ts_val = ts / 1000 if ts > 10_000_000_000 else ts
            return datetime.fromtimestamp(ts_val, tz=timezone.utc)
        if isinstance(ts, datetime):
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    return None


# =============================================================================
# NumBot Menu Parser
# =============================================================================
#
# section_1, section_2 → list of bullet items  (• item)
# section_3            → raw string (everything between 3️⃣➕ and next header)
#                        supports both bullet lists AND free-form text
# section_4            → raw string (everything between 4️⃣➕ and next header)
#
# This mirrors auto_reply_bot.py behaviour exactly.

def parse_numbot_menu(welcome_message: str) -> dict:
    menu_data = {
        'welcome':   '',
        'main_menu': {},
        'section_1': [],    # list of strings (bullet items)
        'section_2': [],    # list of strings (bullet items)
        'section_3': '',    # raw string — dynamic from DB (bullets OR free text)
        'section_4': '',    # raw string — dynamic from DB
    }

    lines           = welcome_message.split('\n')
    current_section = None
    welcome_lines   = []
    in_main_menu    = False
    main_menu_ended = False

    # Temporary accumulators for multi-line raw sections
    _s3_lines: list[str] = []
    _s4_lines: list[str] = []

    for line in lines:
        line = line.strip()

        # ── Detect Main Menu block ────────────────────────────────
        if '📱 Main Menu:' in line or 'Main Menu:' in line:
            in_main_menu    = True
            current_section = 'main'
            continue

        # ── Collect welcome greeting (before Main Menu) ───────────
        if not in_main_menu and line and '====' not in line:
            welcome_lines.append(line)

        # ── Parse main menu items (1️⃣ 2️⃣ 3️⃣ 4️⃣ WITHOUT ➕) ─────
        if in_main_menu and not main_menu_ended:
            if '➕' in line:
                main_menu_ended = True
                # fall through to section-header detection below
            else:
                for num in ['1', '2', '3', '4']:
                    prefix = f"{num}️⃣"
                    if line.startswith(prefix) or line.startswith(num + ' '):
                        item = re.sub(rf'^{num}[️⃣\s]*[→-]?\s*', '', line).strip()
                        if item:
                            menu_data['main_menu'][num] = item
                if line.startswith('===='):
                    main_menu_ended = True
                continue

        # ── Section header detection ──────────────────────────────
        if   '1️⃣➕' in line or '1️⃣ +' in line: current_section = 'section_1'; continue
        elif '2️⃣➕' in line or '2️⃣ +' in line: current_section = 'section_2'; continue
        elif '3️⃣➕' in line or '3️⃣ +' in line: current_section = 'section_3'; continue
        elif '4️⃣➕' in line or '4️⃣ +' in line: current_section = 'section_4'; continue
        elif '📋 Options:' in line or line.startswith('===='):
            current_section = 'options'
            continue

        # ── Collect content per section ───────────────────────────
        if current_section == 'section_1' and line.startswith('•'):
            menu_data['section_1'].append(line.replace('•', '').strip())

        elif current_section == 'section_2' and line.startswith('•'):
            menu_data['section_2'].append(line.replace('•', '').strip())

        elif current_section == 'section_3' and line:
            # Accept ALL non-empty lines (bullets AND free text)
            _s3_lines.append(line)

        elif current_section == 'section_4' and line and '====' not in line and 'Options' not in line:
            # Accept ALL non-empty lines
            _s4_lines.append(line)

    # Join multi-line raw sections into single strings
    menu_data['section_3'] = '\n'.join(_s3_lines).strip()
    menu_data['section_4'] = '\n'.join(_s4_lines).strip()
    menu_data['welcome']   = '\n'.join(welcome_lines).strip()

    return menu_data


# =============================================================================
# Menu Senders  (async with REPLY_DELAY)
# =============================================================================

async def _send(client: NewClient, jid: JID, text: str) -> None:
    """Send a message after REPLY_DELAY seconds (non-blocking async sleep)."""
    await asyncio.sleep(REPLY_DELAY)
    client.send_message(jid, text)


async def send_main_menu(client: NewClient, jid: JID, menu_data: dict) -> None:
    text = (menu_data['welcome'] + '\n\n') if menu_data['welcome'] else ''
    text += '📱 Main Menu:\n\n'
    for k in ['1', '2', '3', '4']:
        if k in menu_data['main_menu']:
            text += f"{k}️⃣ {menu_data['main_menu'][k]}\n"
    text += '\n📌 Choose a number to continue'
    await _send(client, jid, text)


async def send_section_1(client: NewClient, jid: JID, menu_data: dict) -> None:
    name = menu_data['main_menu'].get('1', 'Services')
    text = f"1️⃣ {name}:\n\n"
    for item in menu_data['section_1']:
        text += f"• {item}\n"
    text += "\n📋 Options:\n"
    for k in ['2', '3', '4']:
        if menu_data['main_menu'].get(k):
            text += f"{k} → {menu_data['main_menu'][k]}\n"
    text += "0 → Back to Main Menu\n"
    await _send(client, jid, text)


async def send_section_2(client: NewClient, jid: JID, menu_data: dict) -> None:
    name = menu_data['main_menu'].get('2', 'Prices')
    text = f"2️⃣ {name}:\n\n"
    for item in menu_data['section_2']:
        text += f"• {item}\n"
    text += "\n📋 Options:\n"
    for k in ['3', '4']:
        if menu_data['main_menu'].get(k):
            text += f"{k} → {menu_data['main_menu'][k]}\n"
    text += "0 → Back to Main Menu\n"
    await _send(client, jid, text)


async def send_section_3(client: NewClient, jid: JID, menu_data: dict) -> None:
    name = menu_data['main_menu'].get('3', 'Booking')
    # Read dynamic content directly from menu_data (string, supports any format)
    body = menu_data.get('section_3', '').strip()
    if not body:
        body = "📋 Please leave us your booking details."
    text = f"3️⃣ {name}:\n\n{body}\n\n"
    if menu_data['main_menu'].get('4'):
        text += f"📋 Options:\n4 → {menu_data['main_menu']['4']}\n"
    text += "0 → Back to Main Menu\n"
    await _send(client, jid, text)


async def send_section_4(client: NewClient, jid: JID, menu_data: dict) -> None:
    name = menu_data['main_menu'].get('4', 'Customer Service')
    # Read dynamic content directly from menu_data (string, supports any format)
    body = menu_data.get('section_4', '').strip()
    if not body:
        body = "🕐 Please wait, someone is going to contact you soon..."
    text = f"4️⃣ {name}:\n\n{body}\n\n📋 Options:\n0 → Back to Main Menu\n"
    await _send(client, jid, text)


# =============================================================================
# Core Async Message Handler
# =============================================================================

async def handle_message_async(client: NewClient, msg: MessageEv,
                                bot_id: str, phone: str) -> None:
    """
    Full async handler — mirrors handle_message() in auto_reply_bot.py exactly.
    Runs inside the bot's dedicated asyncio event loop — never blocks neonize.

    Navigation state machine:
      'main'      → user sees main menu, expects 1/2/3/4
      'section_1' → user in section 1,  expects 2/3/4/0
      'section_2' → user in section 2,  expects 3/4/0
      'section_3' → user in section 3,  expects 4/0
      'section_4' → user in section 4,  expects 0
    """

    # ── 1. Private-only filter ────────────────────────────────────────────────
    if not _is_private_message(msg):
        return

    # ── 1b. Stale-message guard (Timestamp Filter) ──────────────────────────
    msg_time = _get_message_timestamp(msg)
    if msg_time is not None:
        if msg_time < PROCESS_START_TIME:
            print(f"[WhatsApp] ⏭ skipping message from before bot start ({msg_time.isoformat()})")
            return
        age_seconds = (datetime.now(timezone.utc) - msg_time).total_seconds()
        if age_seconds > MAX_MESSAGE_AGE_SECONDS:
            print(f"[WhatsApp] ⏭ skipping stale message ({age_seconds:.0f}s old)")
            return
    else:
        # See the warning in _get_message_timestamp()'s docstring — this
        # means the timestamp field shape wasn't recognized. Doesn't block
        # the message (fails safe), but flags that this needs a look.
        print("[WhatsApp] ⚠ could not read message timestamp — processing anyway, see _get_message_timestamp()")

    sender_id = _get_sender_id(msg)
    msg_text  = _extract_text(msg).strip()
    reply_jid = _get_reply_jid(msg)

    print(f"[WhatsApp] New message from +{sender_id}: '{msg_text}'")

    if not reply_jid:
        return

    # ── 2. Fresh DB read on every message ────────────────────────────────────
    try:
        result = (
            supabase.table('telegram_bot')
            .select('id, welcome_message, bot_type, status')
            .eq('id', bot_id)
            .limit(1)
            .execute()
        )
    except Exception as e:
        print(f"  ✗ DB error: {e}")
        return

    if not result.data:
        return

    row = result.data[0]
    if row.get('status') != 'active':
        return

    bot_type        = row.get('bot_type', 'welcome_bot')
    welcome_message = row.get('welcome_message', '')

    # ── 3. Welcome Bot ────────────────────────────────────────────────────────
    if bot_type == 'welcome_bot':
        await _send(client, reply_jid, welcome_message)
        return

    # ── 4. Number Bot ─────────────────────────────────────────────────────────
    menu_data = parse_numbot_menu(welcome_message)
    state     = user_states[bot_id].get(sender_id, 'main')

    # ── Navigation map: digit → (state_name, sender_function) ──────────────────
    _sections = {
        '1': ('section_1', send_section_1),
        '2': ('section_2', send_section_2),
        '3': ('section_3', send_section_3),
        '4': ('section_4', send_section_4),
    }

    # ── Global rule: 1/2/3/4 jumps to that section FROM ANYWHERE ─────────────
    # This lets users freely jump between sections without being stuck.
    if msg_text in _sections:
        # Only navigate to sections that actually exist in the menu
        new_state, fn = _sections[msg_text]
        if msg_text in menu_data['main_menu']:
            user_states[bot_id][sender_id] = new_state
            await fn(client, reply_jid, menu_data)
        else:
            # Section number pressed but not defined in this bot's menu
            user_states[bot_id][sender_id] = 'main'
            await send_main_menu(client, reply_jid, menu_data)
        return

    # ── Global rule: 0 / greeting keywords → always reset to main menu ───────
    if msg_text == '0' or msg_text.lower() in ['start', 'hi', 'hello', '/start', 'hey', 'help']:
        user_states[bot_id][sender_id] = 'main'
        await send_main_menu(client, reply_jid, menu_data)
        return

    # ── Global rule: anything else (random text/numbers) → main menu ─────────
    user_states[bot_id][sender_id] = 'main'
    await send_main_menu(client, reply_jid, menu_data)


# =============================================================================
# Per-Bot Client Factory
# =============================================================================

def make_whatsapp_client(bot_record: dict, tmp_dir: str) -> tuple[NewClient, asyncio.AbstractEventLoop]:
    """
    Creates a neonize NewClient for one bot.

    Threading model:
      neonize callbacks are synchronous (C thread).  We give each bot its own
      asyncio event loop running in a daemon thread.  Every message is handed
      off via asyncio.run_coroutine_threadsafe() — never blocking neonize,
      never blocking other bots.
    """
    bot_id  = str(bot_record['id'])
    phone   = bot_record.get('Phone Number', bot_id[:8])
    db_path = os.path.join(tmp_dir, "whatsapp_session.sqlite3")

    # Dedicated asyncio loop for this bot
    loop = asyncio.new_event_loop()
    def _run_loop(lp: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(lp)
        lp.run_forever()
    threading.Thread(target=_run_loop, args=(loop,), daemon=True,
                     name=f"wa-loop-{bot_id[:8]}").start()

    user_states[bot_id] = {}
    client = NewClient(db_path)

    @client.qr
    def on_qr(_c: NewClient, qr_data: bytes) -> None:
        print(f"\n  Scan QR with WhatsApp → Settings → Linked Devices")

    @client.event(ConnectedEv)
    def on_connected(_c: NewClient, _ev: ConnectedEv) -> None:
        save_session_to_supabase(db_path, bot_id)
        print(f"  ✓ {phone} connected & listening")

    @client.event(MessageEv)
    def on_message(_c: NewClient, msg: MessageEv) -> None:
        # Sync callback — dispatch immediately to async loop, return fast
        asyncio.run_coroutine_threadsafe(
            handle_message_async(_c, msg, bot_id, phone),
            loop
        )

    return client, loop


# =============================================================================
# Bot Lifecycle Helpers  (Dynamic SaaS mode)
# =============================================================================

def _start_bot(rec: dict) -> bool:
    """
    Starts a single bot from a Supabase record.
    Idempotent — skips bots already in the registry.
    Returns True if started successfully.
    """
    bot_id   = str(rec['id'])
    bot_name = rec.get('bot_name', 'Unknown')

    with _registry_lock:
        if bot_id in _registry:
            return False  # already running

    if not (rec.get('Send Code') or '').strip():
        return False  # no session yet

    try:
        tmp_dir = tempfile.mkdtemp(prefix=f"wa_{bot_id[:8]}_")
        if not load_session_from_supabase(bot_id, tmp_dir):
            return False

        client, loop = make_whatsapp_client(rec, tmp_dir)
        t = threading.Thread(target=client.connect, daemon=True,
                             name=f"wa-{bot_id[:8]}")
        t.start()

        with _registry_lock:
            _registry[bot_id] = {
                'client':   client,
                'thread':   t,
                'loop':     loop,
                'tmp_dir':  tmp_dir,
                'bot_name': bot_name,
            }

        print(f"  ▶  {bot_name} | Type: {rec.get('bot_type', 'welcome_bot')}")
        return True

    except Exception as e:
        print(f"  ✗ Failed to start {bot_name}: {e}")
        return False


def _stop_bot(bot_id: str) -> None:
    """Stops a running bot and removes it from the registry."""
    with _registry_lock:
        entry = _registry.pop(bot_id, None)
    if entry is None:
        return
    bot_name = entry.get('bot_name', bot_id[:8])
    try:
        entry['client'].disconnect()
    except Exception:
        pass
    try:
        entry['loop'].call_soon_threadsafe(entry['loop'].stop)
    except Exception:
        pass
    user_states.pop(bot_id, None)
    print(f"  ■  {bot_name} stopped")


def _sync_bots() -> None:
    """
    Polls Supabase and reconciles the running bot registry:
      - New active bots with sessions  → start them
      - Bots that became inactive/deleted → stop them
    """
    try:
        response = (
            supabase.table('telegram_bot')
            .select('*')
            .eq('status', 'active')
            .eq('platform', 'whatsapp')
            .not_.is_('bot_name', 'null')
            .execute()
        )
    except Exception as e:
        print(f"  ✗ Supabase poll error: {e}")
        return

    active_ids: set[str] = set()
    if response.data:
        for rec in response.data:
            bot_id = str(rec['id'])
            active_ids.add(bot_id)
            _start_bot(rec)  # no-op if already running

    # Stop bots that disappeared (deleted or deactivated)
    with _registry_lock:
        running_ids = list(_registry.keys())
    for bot_id in running_ids:
        if bot_id not in active_ids:
            _stop_bot(bot_id)


def _extract_new_record(payload: dict):
    """
    Supabase Realtime payloads for postgres_changes carry the changed row
    under slightly different keys depending on client/library version
    (seen in the wild: payload['data']['record'], payload['record'],
    payload['new']). This tries the common shapes rather than assuming
    one, and returns None if none match.
    """
    for key_path in (('data', 'record'), ('record',), ('new',), ('data', 'new')):
        node = payload
        try:
            for k in key_path:
                node = node[k]
            if node:
                return node
        except (KeyError, TypeError):
            continue
    return None


async def _start_realtime_listener_async() -> None:
    """
    Subscribes to Postgres changes on the telegram_bot table (filtered to
    platform=whatsapp) via Supabase Realtime, so a newly-activated bot
    starts within ~1 second instead of waiting for the next poll.

    REQUIRES a one-time manual step in the Supabase dashboard: enable
    Replication for the `telegram_bot` table (Database → Replication →
    toggle it on), or run:
        alter publication supabase_realtime add table telegram_bot;
    Telegram and WhatsApp bots share this same table (filtered by the
    `platform` column) — if you already enabled this for the Telegram
    bot, WhatsApp is covered too, no extra step needed here.

    _start_bot()/_stop_bot() in this file are plain synchronous functions
    (this file manages bots with threads, not asyncio) — the callback
    below calls them directly, which is safe since they're already
    protected by _registry_lock the same way _polling_loop() uses them.
    """
    try:
        realtime_client: AsyncClient = await acreate_client(
            os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY')
        )
    except Exception as e:
        print(f"⚠  Realtime listener could not start ({e}) — relying on {POLL_INTERVAL}s polling only")
        return

    async def _on_change(payload: dict) -> None:
        try:
            record = _extract_new_record(payload)
            if record is None:
                print(f"⚠  Realtime: unrecognized payload shape, ignoring: {payload!r}")
                return
            if record.get('platform') != 'whatsapp' or record.get('status') != 'active':
                return
            if not record.get('bot_name'):
                return
            bot_id = str(record.get('id') or '')
            if not bot_id:
                return
            with _registry_lock:
                already_running = bot_id in _registry
            if already_running:
                return
            print(f"⚡ Realtime: bot {bot_id} activated — starting now")
            _start_bot(record)  # sync function — called directly, not awaited
        except Exception as e:
            print(f"⚠  Realtime callback error: {e}")

    channel = realtime_client.channel('whatsapp-bot-changes')
    channel.on_postgres_changes(
        '*', schema='public', table='telegram_bot',
        filter='platform=eq.whatsapp',
        callback=lambda payload: asyncio.create_task(_on_change(payload)),
    )
    await channel.subscribe()
    print("📡 Realtime listener active — new WhatsApp bots will start within ~1s of activation")

    # Keep this thread's own event loop alive indefinitely
    await asyncio.Event().wait()


def _run_realtime_listener_in_thread() -> None:
    """
    Runs the async Supabase Realtime listener inside its own asyncio
    event loop, in its own background thread. Needed because the rest of
    this file manages bots synchronously with threads, but supabase-py's
    realtime client (acreate_client + channel.subscribe()) is async-only.
    """
    try:
        asyncio.run(_start_realtime_listener_async())
    except Exception as e:
        print(f"⚠  Realtime listener thread crashed ({e}) — relying on {POLL_INTERVAL}s polling only")


def _polling_loop(stop_event: threading.Event) -> None:
    """
    Background daemon thread: polls Supabase every POLL_INTERVAL seconds.
    Now a safety net behind the realtime listener above — see
    _start_realtime_listener_async() for the primary, fast path.
    Cleanly stoppable via stop_event.
    """
    while not stop_event.is_set():
        stop_event.wait(POLL_INTERVAL)
        if stop_event.is_set():
            break
        _sync_bots()
        with _registry_lock:
            count = len(_registry)
        if count:
            print(f"  ↻  Poll: {count} bot(s) active")


# =============================================================================
# Run All Active WhatsApp Bots  (Dynamic SaaS mode)
# =============================================================================

def run_all_whatsapp_bots() -> None:
    print("\n" + "="*42)
    print("      WHATSAPP AUTO-REPLY BOT")
    print("="*42)
    print(f"  Groups       : IGNORED")
    print(f"  Status       : ACTIVE")
    print(f"  DB Refresh   : EVERY MESSAGE")
    print(f"  Poll Interval: {POLL_INTERVAL}s")
    print("="*42 + "\n")

    # Initial sync
    _sync_bots()

    with _registry_lock:
        count = len(_registry)

    if count == 0:
        print("  ⚠  No bots started yet (waiting for new activations)\n")
    else:
        print(f"\n  {count} bot(s) live | Private only")

    print("="*42)

    # Background poller (safety net)
    stop_event = threading.Event()
    poller = threading.Thread(target=_polling_loop, args=(stop_event,),
                              daemon=True, name="wa-poller")
    poller.start()

    # Realtime listener (primary, fast path)
    threading.Thread(target=_run_realtime_listener_in_thread, daemon=True, name="wa-realtime").start()

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n  Shutting down...")
        stop_event.set()
        with _registry_lock:
            ids = list(_registry.keys())
        for bot_id in ids:
            _stop_bot(bot_id)


# =============================================================================
# Programmatic API  (called from server.py)
# =============================================================================

def run_whatsapp_bot_for_user(user_id: str) -> Optional[NewClient]:
    """
    Start the WhatsApp bot for a specific user_id (non-blocking, idempotent).
    Returns the NewClient, or None if no bot/session found.
    """
    result = (
        supabase.table('telegram_bot')
        .select('*')
        .eq('user_id', user_id)
        .eq('platform', 'whatsapp')
        .eq('status', 'active')
        .not_.is_('bot_name', 'null')
        .limit(1)
        .execute()
    )
    if not result.data:
        return None

    rec    = result.data[0]
    bot_id = str(rec['id'])

    # Already running — return existing client
    with _registry_lock:
        if bot_id in _registry:
            return _registry[bot_id]['client']

    started = _start_bot(rec)
    if not started:
        return None

    with _registry_lock:
        return _registry.get(bot_id, {}).get('client')


def stop_whatsapp_bot_for_user(user_id: str) -> bool:
    """
    Stop the WhatsApp bot for a specific user_id.
    Called from server.py on bot deletion.
    """
    with _registry_lock:
        ids = list(_registry.keys())

    for bot_id in ids:
        try:
            result = (
                supabase.table('telegram_bot')
                .select('user_id')
                .eq('id', bot_id)
                .limit(1)
                .execute()
            )
            if result.data and str(result.data[0].get('user_id')) == str(user_id):
                _stop_bot(bot_id)
                return True
        except Exception:
            pass
    return False


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        uid    = sys.argv[1]
        print(f"\nSingle-user mode: {uid}")
        client = run_whatsapp_bot_for_user(uid)
        if client:
            try:
                threading.Event().wait()
            except KeyboardInterrupt:
                print("\nStopped.")
    else:
        run_all_whatsapp_bots()
