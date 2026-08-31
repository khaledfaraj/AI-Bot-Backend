from telethon import TelegramClient, events
from telethon.sessions import StringSession
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import asyncio 
import re

import threading

from session_crypto import decrypt_text_safe

load_dotenv()

# Silence noisy library logs
import logging
logging.getLogger("telethon").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.ERROR)

# ─────────────────────────────────────────────────────────────────────────
# 🌐 Minimal health-check HTTP server (Render / uptime-pinger friendly)
# ─────────────────────────────────────────────────────────────────────────
# Render's free plan needs the process to bind an HTTP port and answer
# requests, or it's treated as crashed and gets restarted. This does NOT
# touch the bot's own polling logic below at all — it's a completely
# separate, minimal server running in its own background thread, using
# only the stdlib (http.server), so no new dependency is added.
#
# Started here (top of file, at import time) rather than inside
# `if __name__ == '__main__':` so the port is already open and answering
# before the (potentially slow) initial Supabase sync / Telegram logins
# even begin — avoiding a failed health check on first boot.
import http.server

_HEALTH_PORT = int(os.getenv('PORT', 8081))  # Render injects PORT; 8081 is just the local fallback


class _HealthCheckHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK - telegram bot running')

    def log_message(self, format, *args):
        pass  # don't spam the bot's console with per-ping access logs


def _start_health_check_server() -> None:
    server = http.server.HTTPServer(('0.0.0.0', _HEALTH_PORT), _HealthCheckHandler)
    print(f"🌐 Health-check server listening on 0.0.0.0:{_HEALTH_PORT}")
    server.serve_forever()


threading.Thread(target=_start_health_check_server, daemon=True, name="health-check").start()

supabase: Client = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

# user_states[bot_id][sender_id] = current menu state
user_states = {}

REPLY_DELAY   = 3   # seconds before every auto-reply
POLL_INTERVAL = 30  # seconds between Supabase polls for new/stopped bots

# =============================================================================
# Bot Registry  — tracks every running Telethon client
# =============================================================================
# {bot_id: {'client': TelegramClient, 'phone': str, 'bot_name': str}}
_registry: dict = {}
_registry_lock  = threading.Lock()


# ============================================
# Helper: send with delay
# ============================================
async def send_delayed(event, text, delay=REPLY_DELAY):
    await asyncio.sleep(delay)
    await event.respond(text)


# ============================================
# Parse welcome_message into menu sections
# ============================================
def parse_numbot_menu(welcome_message):
    menu_data = {
        'welcome':   '',
        'main_menu': {},
        'section_1': [],   # bullet list
        'section_2': [],   # bullet list
        'section_3': '',   # raw string (bullets OR free text)
        'section_4': '',   # raw string
    }
    _s3_lines: list = []
    _s4_lines: list = []

    lines = welcome_message.split('\n')
    current_section = None
    welcome_lines = []
    in_main_menu = False
    main_menu_ended = False

    for line in lines:
        line = line.strip()

        # ── Detect start of Main Menu block ──────────────────────
        if '📱 Main Menu:' in line or 'Main Menu:' in line:
            in_main_menu = True
            current_section = 'main'
            continue

        # ── Collect greeting (before Main Menu) ──────────────────
        if not in_main_menu and line and '====' not in line:
            welcome_lines.append(line)

        # ── Parse main menu items (1️⃣ 2️⃣ 3️⃣ 4️⃣ WITHOUT ➕) ─────
        if in_main_menu and not main_menu_ended:
            if '➕' in line:
                main_menu_ended = True
                # fall through so this line is checked as a section header
            else:
                if line.startswith('1️⃣') or line.startswith('1 '):
                    item = re.sub(r'^1[️⃣\s]*[→-]?\s*', '', line).strip()
                    if item: menu_data['main_menu']['1'] = item
                elif line.startswith('2️⃣') or line.startswith('2 '):
                    item = re.sub(r'^2[️⃣\s]*[→-]?\s*', '', line).strip()
                    if item: menu_data['main_menu']['2'] = item
                elif line.startswith('3️⃣') or line.startswith('3 '):
                    item = re.sub(r'^3[️⃣\s]*[→-]?\s*', '', line).strip()
                    if item: menu_data['main_menu']['3'] = item
                elif line.startswith('4️⃣') or line.startswith('4 '):
                    item = re.sub(r'^4[️⃣\s]*[→-]?\s*', '', line).strip()
                    if item: menu_data['main_menu']['4'] = item
                elif line.startswith('===='):
                    main_menu_ended = True
                continue

        # ── Detect section headers (ALL must have ➕) ─────────────
        # FIX: section 3 now correctly detected with ➕ (was broken before)
        if   '1️⃣➕' in line or '1️⃣ +' in line:
            current_section = 'section_1'; continue
        elif '2️⃣➕' in line or '2️⃣ +' in line:
            current_section = 'section_2'; continue
        elif '3️⃣➕' in line or '3️⃣ +' in line:          # ← FIXED (was wrong before)
            current_section = 'section_3'; continue
        elif '4️⃣➕' in line or '4️⃣ +' in line:
            current_section = 'section_4'; continue
        elif '📋 Options:' in line or line.startswith('===='):
            current_section = 'options'; continue

        # ── Parse bullet items per section ────────────────────────
        if current_section == 'section_1' and line.startswith('•'):
            menu_data['section_1'].append(line.replace('•', '').strip())
        elif current_section == 'section_2' and line.startswith('•'):
            menu_data['section_2'].append(line.replace('•', '').strip())
        elif current_section == 'section_3' and line:
            _s3_lines.append(line)
        elif current_section == 'section_4' and line and '====' not in line and 'Options' not in line:
            _s4_lines.append(line)

    menu_data['section_3'] = '\n'.join(_s3_lines).strip()
    menu_data['section_4'] = '\n'.join(_s4_lines).strip()
    menu_data['welcome']   = '\n'.join(welcome_lines).strip()
    return menu_data


# ============================================
# Send sections (with real names from menu_data)
# ============================================
async def send_main_menu(event, menu_data):
    text = ''
    if menu_data['welcome']:
        text += menu_data['welcome'] + '\n\n'
    text += '📱 Main Menu:\n\n'
    for k in ['1', '2', '3', '4']:
        if k in menu_data['main_menu']:
            text += f"{k}️⃣ {menu_data['main_menu'][k]}\n"
    text += '\n📌 Choose a number to continue'
    await send_delayed(event, text)


async def send_section_1(event, menu_data):
    name = menu_data['main_menu'].get('1', 'Services')
    text = f"1️⃣ {name}:\n\n"
    for item in menu_data['section_1']:
        text += f"• {item}\n"
    text += "\n📋 Options:\n"
    for k, n in [('2','2'),('3','3'),('4','4')]:
        if menu_data['main_menu'].get(k):
            text += f"{k} → {menu_data['main_menu'][k]}\n"
    text += "0 → Back to Main Menu\n"
    await send_delayed(event, text)


async def send_section_2(event, menu_data):
    name = menu_data['main_menu'].get('2', 'Prices')
    text = f"2️⃣ {name}:\n\n"
    for item in menu_data['section_2']:
        text += f"• {item}\n"
    text += "\n📋 Options:\n"
    for k, n in [('3','3'),('4','4')]:
        if menu_data['main_menu'].get(k):
            text += f"{k} → {menu_data['main_menu'][k]}\n"
    text += "0 → Back to Main Menu\n"
    await send_delayed(event, text)


async def send_section_3(event, menu_data):
    name = menu_data['main_menu'].get('3', 'Booking')
    body = menu_data.get('section_3', '').strip()
    if not body:
        body = "📋 Please leave us your booking details."
    text  = f"3️⃣ {name}:\n\n{body}\n\n"
    if menu_data['main_menu'].get('4'):
        text += f"📋 Options:\n4 → {menu_data['main_menu']['4']}\n"
    text += "0 → Back to Main Menu\n"
    await send_delayed(event, text)


async def send_section_4(event, menu_data):
    name = menu_data['main_menu'].get('4', 'Customer Service')
    body = menu_data.get('section_4', '').strip()
    if not body:
        body = "🕐 Please wait, someone is going to contact you soon..."
    text  = f"4️⃣ {name}:\n\n{body}\n\n📋 Options:\n0 → Back to Main Menu\n"
    await send_delayed(event, text)


# ============================================
# Create one bot handler for a single record
# ============================================
def make_handler(bot_record):
    bot_id = bot_record.get('id')
    encrypted_session = bot_record.get('Send Code', '')
    session_string    = decrypt_text_safe(encrypted_session, context=f"bot {bot_id}")
    api_id            = bot_record.get('API ID', '')
    api_hash          = bot_record.get('API Hash', '')
    phone             = bot_record.get('Phone Number', 'unknown')

    try:
        api_id_int = int(str(api_id).strip())
    except (ValueError, TypeError):
        print(f"[Bot {phone}] ✗ Invalid API ID: {api_id!r}")
        return None
    client = TelegramClient(StringSession(session_string), api_id_int, str(api_hash).strip())

    if bot_id not in user_states:
        user_states[bot_id] = {}

    @client.on(events.NewMessage(incoming=True))
    async def handle_message(event):
        # ── FIX 2: Private messages ONLY — ignore all groups ───────
        if not event.is_private:
            return

        # ── FIX 5: sender can legitimately be None (deleted account,
        # certain channel-relayed messages) — guard before touching it.
        sender = await event.get_sender()
        if sender is None:
            return
        if getattr(sender, 'bot', False):
            return

        sender_id   = sender.id
        sender_name = getattr(sender, 'first_name', 'User') or 'User'
        msg_text    = (event.message.text or "").strip()

        print(f"[Bot {phone}] 📨 {sender_name}: {msg_text!r}")

        # ── FIX 4: Safe DB fetch — no .single() crash ──────────────
        # .single() raises PGRST116 when 0 rows found; use .limit(1) instead
        #
        # ── FIX 6: run the (synchronous!) supabase-py call in a worker
        # thread. supabase-py's .execute() does blocking network I/O.
        # Calling it directly inside this async handler freezes the
        # ENTIRE asyncio event loop — meaning every other Telegram bot
        # running in this same process stalls until this one DB call
        # returns. asyncio.to_thread() moves the blocking call off the
        # event loop so other bots keep responding while this waits.
        try:
            result = await asyncio.to_thread(
                lambda: supabase.table('telegram_bot')
                    .select('id, welcome_message, bot_type, status')
                    .eq('id', bot_id)
                    .limit(1)
                    .execute()
            )
        except Exception as e:
            print(f"[Bot {phone}] ❌ DB error: {e}")
            return

        if not result.data:
            print(f"[Bot {phone}] ⚠️ Record not found for id={bot_id}")
            return

        row = result.data[0]
        if row.get('status') != 'active':
            print(f"[Bot {phone}] ⏸️ Bot is no longer active")
            return

        bot_type        = row.get('bot_type', 'welcome_bot')
        welcome_message = row.get('welcome_message', '')
        state           = user_states[bot_id].get(sender_id, 'main')

        print(f"[Bot {phone}] 📋 Type: {bot_type} | State: {state}")

        # ── WELCOME BOT ─────────────────────────────────────────────
        if bot_type == 'welcome_bot':
            await send_delayed(event, welcome_message)
            return

        # ── NUMBER BOT — Universal Navigation (identical to WhatsApp) ──
        menu_data = parse_numbot_menu(welcome_message)

        _sections = {
            '1': ('section_1', send_section_1),
            '2': ('section_2', send_section_2),
            '3': ('section_3', send_section_3),
            '4': ('section_4', send_section_4),
        }

        # Rule 1: 1/2/3/4 → jump to that section from ANYWHERE
        if msg_text in _sections:
            new_state, fn = _sections[msg_text]
            if msg_text in menu_data['main_menu']:
                user_states[bot_id][sender_id] = new_state
                await fn(event, menu_data)
            else:
                user_states[bot_id][sender_id] = 'main'
                await send_main_menu(event, menu_data)
            return

        # Rule 2: 0 or greeting keywords → reset to main menu
        if msg_text == '0' or msg_text.lower() in ['start', 'hi', 'hello', '/start', 'hey', 'help']:
            user_states[bot_id][sender_id] = 'main'
            await send_main_menu(event, menu_data)
            return

        # Rule 3: first message OR any unknown input → show main menu
        user_states[bot_id][sender_id] = 'main'
        await send_main_menu(event, menu_data)

    return client


# =============================================================================
# Bot Lifecycle Helpers
# =============================================================================

async def _start_bot(rec: dict) -> bool:
    """
    Starts a single Telegram bot from a Supabase record.
    Idempotent — skips bots already in the registry.
    Returns True if started successfully.
    """
    bot_id   = str(rec.get('id'))
    bot_name = rec.get('bot_name', 'Unknown')
    phone    = rec.get('Phone Number', 'unknown')

    with _registry_lock:
        if bot_id in _registry:
            return False  # already running

    session_string = (rec.get('Send Code') or '').strip()
    if not session_string:
        print(f"  ⚠  {bot_name}: no session string (pending verification)")
        return False

    api_id   = rec.get('API ID', '')
    api_hash = rec.get('API Hash', '')

    # Robust check: handle both int and string from Supabase
    if not str(api_id).strip() or not str(api_hash).strip():
        print(f"  ✗ Missing API ID or Hash for {bot_name}")
        return False

    try:
        client = make_handler(rec)
        if client is None:
            return False

        # ── FIX 7: client.start() on an invalid/corrupted StringSession
        # (no phone/code_callback given) can hang waiting for interactive
        # login input that will never come — freezing this coroutine
        # forever and, since _sync_bots() awaits _start_bot() directly,
        # blocking every other bot from starting or syncing too. A
        # timeout turns a permanent hang into a loud, recoverable failure.
        try:
            await asyncio.wait_for(client.start(), timeout=30)
        except asyncio.TimeoutError:
            print(f"  ✗ {bot_name} ({phone}): login timed out — session is likely invalid/expired")
            try:
                await client.disconnect()
            except Exception:
                pass
            return False

        with _registry_lock:
            _registry[bot_id] = {
                'client':   client,
                'phone':    phone,
                'bot_name': bot_name,
            }

        # Run bot in background task — does not block the polling loop
        asyncio.create_task(_run_bot_until_disconnected(bot_id, client))
        print(f"  ▶  {bot_name} | {phone} | Type: {rec.get('bot_type','?')}")
        return True

    except Exception as e:
        print(f"  ✗ Failed to start {bot_name} ({phone}): {e}")
        return False


async def _run_bot_until_disconnected(bot_id: str, client: TelegramClient) -> None:
    """
    Runs a bot's connection until it drops (network blip, ISP/MTProto
    filtering, Telegram terminating the session, etc.), then cleans up
    the registry entry.

    ── FIX 8 (the main cause of "bot silently stops replying") ──
    Previously nothing removed `bot_id` from `_registry` when the
    connection died. `_start_bot()` refuses to (re)start a bot that's
    already in `_registry`, so once a bot disconnected even once it
    stayed marked as "running" forever and was never retried — the
    only fix was restarting the whole process. Clearing it here lets
    the next `_sync_bots()` poll (≤ POLL_INTERVAL seconds later) bring
    it back automatically.
    """
    try:
        await client.run_until_disconnected()
    finally:
        with _registry_lock:
            _registry.pop(bot_id, None)
        user_states.pop(bot_id, None)
        print(f"  ⚠  Bot {bot_id} disconnected — will retry on next poll (≤{POLL_INTERVAL}s)")


async def _stop_bot(bot_id: str) -> None:
    """
    Disconnects a running bot and removes it from the registry.
    """
    with _registry_lock:
        entry = _registry.pop(bot_id, None)

    if entry is None:
        return

    bot_name = entry.get('bot_name', bot_id[:8])
    phone    = entry.get('phone', '')
    try:
        await entry['client'].disconnect()
    except Exception:
        pass
    user_states.pop(bot_id, None)
    print(f"  ■  {bot_name} | {phone} stopped")


async def _sync_bots() -> None:
    """
    Polls Supabase and reconciles the running bot registry:
      - New active bots with sessions  → start them
      - Bots that became inactive/deleted → stop them
    """
    try:
        # ── FIX 6 (same class as above): this poll runs every 30s and
        # was blocking the whole event loop — meaning ALL running bots
        # would freeze for the duration of this one query. Off-load it.
        response = await asyncio.to_thread(
            lambda: supabase.table('telegram_bot')
                .select('*')
                .eq('status', 'active')
                .eq('platform', 'telegram')
                .execute()
        )
    except Exception as e:
        print(f"  ✗ Supabase poll error: {e}")
        return

    active_ids = set()
    if response.data:
        total = len(response.data)
        bots_with_session = [r for r in response.data if (r.get('Send Code') or '').strip()]
        if total != len(bots_with_session):
            print(f"  ⚠  {total - len(bots_with_session)} bot(s) skipped (no session)")
        for rec in bots_with_session:
            bot_id = str(rec.get('id'))
            active_ids.add(bot_id)
            await _start_bot(rec)  # no-op if already running

    # Stop bots that disappeared (deleted or deactivated)
    with _registry_lock:
        running_ids = list(_registry.keys())

    for bot_id in running_ids:
        if bot_id not in active_ids:
            await _stop_bot(bot_id)


async def _polling_loop() -> None:
    """
    Async background task: polls Supabase every POLL_INTERVAL seconds.
    Runs inside the main asyncio event loop alongside all active bots.
    """
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        await _sync_bots()
        with _registry_lock:
            count = len(_registry)
        if count:
            print(f"  ↻  Poll: {count} bot(s) active")


# =============================================================================
# Run ALL active Telegram bots  (Dynamic SaaS mode)
# =============================================================================

async def run_all_bots() -> None:
    # Initial sync — start all bots already active in Supabase
    await _sync_bots()

    with _registry_lock:
        count = len(_registry)

    if count == 0:
        print("  ⚠  No bots started yet — waiting for new activations\n")
    else:
        print(f"\n  {count} bot(s) live | {REPLY_DELAY}s delay | Private only")

    print("=" * 55)

    # Launch background poller as a concurrent async task
    asyncio.create_task(_polling_loop())

    # Keep the event loop alive indefinitely
    await asyncio.Event().wait()


# =============================================================================
# Main
# =============================================================================
if __name__ == '__main__':
    print("\n" + "=" * 55)
    print("🤖  TELEGRAM AUTO-REPLY BOT")
    print("=" * 55)
    print(f"⏱️  Delay        : {REPLY_DELAY}s")
    print(f"🔒  Groups       : IGNORED")
    print(f"🔄  DB refresh   : every message")
    print(f"↻   Poll interval: {POLL_INTERVAL}s")
    print("=" * 55 + "\n")
    try:
        asyncio.run(run_all_bots())
    except KeyboardInterrupt:
        print("\n  Shutting down...")
