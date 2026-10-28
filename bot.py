# --- imports ---
import asyncio
import html
import logging
import os
import random
import re
import time

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import (
    BotCommand,
    FSInputFile,
    InlineKeyboardButton,
    MenuButtonCommands,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import ClientSession, ClientTimeout, web
from dotenv import load_dotenv
from telethon import TelegramClient, utils
from telethon.errors import (
    FloodWaitError,
    InviteRequestSentError,
    RPCError,
    UserPrivacyRestrictedError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import (
    CheckChatInviteRequest,
    ImportChatInviteRequest,
)
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import ChatInviteAlready

try:
    from telethon.tl.functions.payments import GetSavedStarGiftsRequest
except ImportError:
    GetSavedStarGiftsRequest = None

try:
    from telethon.tl.functions.payments import GetUserGiftsRequest
except ImportError:
    GetUserGiftsRequest = None

# --- config ---
load_dotenv()
BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]
MAX_CHATS = int(os.getenv("MAX_CHATS_PER_USER", 3))
MAX_GIFTS = int(os.getenv("MAX_GIFTS_FOR_SELLER", 50))
DELAY_MIN = float(os.getenv("PARSE_DELAY_MIN", 0.2))
DELAY_MAX = float(os.getenv("PARSE_DELAY_MAX", 0.6))
CONCURRENCY = int(os.getenv("PARSE_CONCURRENCY", 8))
ADMIN_ID = int(os.getenv("ADMIN_ID", 6811074441))
ONLY_USERNAME = os.getenv("ONLY_USERNAME", "1") == "1"
HOST = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 8080))
SECRET = os.getenv("WEBHOOK_SECRET", "")
WEBHOOK_PATH = "/webhook"
PING_EVERY = 300
DB = "data.db"
PAGE_SIZE = 20
MENU_SIZE = 10
RENT_FIELDS = ("rented_until", "rent_until", "rented")
WALLET_FIELDS = ("transfer_stars", "from_wallet", "owner_address")
BAD_LINK = "Не получилось войти. Ссылка битая или доступ закрыта."
ERR = "Что-то сломалось. Попробуй ещё раз."
HELP = (
    "Как пользоваться:\n\n"
    "1. /add &lt;ссылка&gt; — до 3 чатов на тебя.\n"
    "2. Ждём, пока парсер пройдётся по участникам.\n"
    "3. /start — выбираешь модель, листаешь, тапаешь на @user и пишешь ему.\n"
    "4. /remove — убрать свой чат (освобождает слот).\n\n"
    "Приватные чаты с одобрением заявки не поддерживаются.\n"
    "Профиль проверяется один раз, повторно не сканируется."
)

bot = None
dp = None
client = None
q = asyncio.Queue()
_db = None
cancelled = set()
awaiting_db = set()
flood_until = 0.0

# --- log ---
logger = logging.getLogger("nftparser")
logger.setLevel(logging.INFO)
line_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
for h in (logging.FileHandler("bot.log", encoding="utf-8"), logging.StreamHandler()):
    h.setFormatter(line_fmt)
    logger.addHandler(h)

# --- db ---
SCHEMA = (
    "CREATE TABLE IF NOT EXISTS chats ( "
    "    id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "    owner_user_id INTEGER, "
    "    chat_id INTEGER UNIQUE, "
    "    username TEXT, "
    "    title TEXT, "
    "    added_at INTEGER, "
    "    status TEXT "
    "); "
    "CREATE TABLE IF NOT EXISTS users ( "
    "    tg_user_id INTEGER PRIMARY KEY, "
    "    username TEXT, "
    "    first_name TEXT, "
    "    rank_ok INTEGER, "
    "    created_at INTEGER "
    "); "
    "CREATE TABLE IF NOT EXISTS scanned_profiles ( "
    "    user_id INTEGER PRIMARY KEY, "
    "    scanned_at INTEGER, "
    "    had_nft INTEGER "
    "); "
    "CREATE TABLE IF NOT EXISTS nfts ( "
    "    id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "    gift_slug TEXT, "
    "    gift_num INTEGER, "
    "    name TEXT, "
    "    model_slug TEXT, "
    "    owner_user_id INTEGER, "
    "    owner_username TEXT, "
    "    nft_link TEXT, "
    "    profile_link TEXT, "
    "    chat_id_source INTEGER, "
    "    created_at INTEGER, "
    "    UNIQUE(gift_slug, gift_num) "
    "); "
    "CREATE TABLE IF NOT EXISTS user_views ( "
    "    user_id INTEGER, "
    "    nft_id INTEGER, "
    "    viewed_at INTEGER, "
    "    PRIMARY KEY (user_id, nft_id) "
    "); "
    "CREATE TABLE IF NOT EXISTS parse_tasks ( "
    "    id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "    chat_id INTEGER, "
    "    status TEXT, "
    "    error TEXT, "
    "    created_at INTEGER, "
    "    updated_at INTEGER "
    "); "
    "CREATE TABLE IF NOT EXISTS settings ( "
    "    key TEXT PRIMARY KEY, "
    "    value TEXT "
    "); "
    "CREATE INDEX IF NOT EXISTS idx_nfts_model ON nfts(model_slug); "
    "CREATE INDEX IF NOT EXISTS idx_nfts_owner ON nfts(owner_user_id); "
    "CREATE INDEX IF NOT EXISTS idx_views_user ON user_views(user_id); "
)


async def init_db():
    global _db
    _db = await aiosqlite.connect(DB)
    await _db.execute("PRAGMA journal_mode=WAL")
    await _db.execute("PRAGMA synchronous=NORMAL")
    await _db.executescript(SCHEMA)
    await _db.commit()


async def db_exec(sql, *args):
    cur = await _db.execute(sql, args)
    await _db.commit()
    return cur.lastrowid


async def db_many(sql, rows):
    if not rows:
        return
    await _db.executemany(sql, rows)
    await _db.commit()


async def db_fetch(sql, *args):
    async with _db.execute(sql, args) as cur:
        return await cur.fetchall()


async def db_fetchone(sql, *args):
    async with _db.execute(sql, args) as cur:
        return await cur.fetchone()


def num(n):
    return f"{n:,}".replace(",", " ")


# --- userbot ---
def parse_link(text):
    text = text.strip()
    m = re.search(r"t\.me/(?:\+|joinchat/)([\w-]+)", text)
    if m:
        return "invite", m.group(1)
    m = re.search(r"t\.me/([A-Za-z0-9_]{4,})", text)
    m = m or re.fullmatch(r"@([A-Za-z0-9_]{4,})", text)
    if m:
        return "public", m.group(1)
    return None


async def join(link):
    kind, key = link
    if kind == "public":
        ent = await client.get_entity(key)
        await client(JoinChannelRequest(ent))
        return ent
    info = await client(CheckChatInviteRequest(key))
    if isinstance(info, ChatInviteAlready):
        return info.chat
    res = await client(ImportChatInviteRequest(key))
    return res.chats[0]


async def add_chat(owner, text):
    row = await db_fetchone("SELECT COUNT(*) FROM chats WHERE owner_user_id=?", owner)
    if row[0] >= MAX_CHATS:
        return f"У тебя уже {MAX_CHATS} чата — это максимум. Больше добавить нельзя."
    link = parse_link(text)
    if not link:
        return BAD_LINK
    try:
        ent = await join(link)
    except InviteRequestSentError:
        return "Не зашёл: там одобрение заявки. Такие чаты не парсятся."
    except FloodWaitError as e:
        logger.warning("join flood %s", e.seconds)
        return ERR
    except (RPCError, ValueError, TypeError) as e:
        logger.warning("join failed: %s", e)
        return BAD_LINK
    title = getattr(ent, "title", None)
    if not title:
        return BAD_LINK
    chat_id = utils.get_peer_id(ent)
    if await db_fetchone("SELECT 1 FROM chats WHERE chat_id=?", chat_id):
        return "Этот чат уже в списке."
    ts = int(time.time())
    await db_exec(
        "INSERT INTO chats(owner_user_id, chat_id, username, title, added_at, status)"
        " VALUES (?,?,?,?,?,?)",
        owner, chat_id, getattr(ent, "username", None), title, ts, "queued",
    )
    await db_exec(
        "INSERT INTO parse_tasks(chat_id, status, created_at, updated_at)"
        " VALUES (?,?,?,?)",
        chat_id, "pending", ts, ts,
    )
    await q.put(chat_id)
    return f"Добавил: {html.escape(title)}. Парсинг в очереди, модели появятся в /start."


# --- gifts ---
def clean(sg, user_id):
    g = getattr(sg, "gift", None)
    slug = getattr(g, "slug", None)
    if not slug:
        return None
    owner = getattr(g, "owner_id", None)
    owner = getattr(owner, "user_id", owner)
    if owner != user_id:
        return None
    for f in RENT_FIELDS + WALLET_FIELDS:
        if getattr(sg, f, None) or getattr(g, f, None):
            return None
    base, _, tail = slug.rpartition("-")
    n = getattr(g, "num", None) or (int(tail) if tail.isdigit() else None)
    if not base or not n:
        return None
    name = getattr(g, "title", None) or base
    return {"slug": base.replace(" ", ""), "num": n, "name": name}


def gifts_request(peer, offset, extra):
    if GetSavedStarGiftsRequest:
        return GetSavedStarGiftsRequest(peer=peer, offset=offset, limit=100, **extra)
    return GetUserGiftsRequest(user_id=peer, offset=offset, limit=100)


async def fetch_gifts(peer, user_id):
    if not (GetSavedStarGiftsRequest or GetUserGiftsRequest):
        logger.warning("в этой версии Telethon нет запроса подарков")
        return []
    out = []
    offset = ""
    extra = {"exclude_unlimited": True}  # обычные подарки не нужны, только NFT
    for _ in range(10):
        try:
            try:
                req = gifts_request(peer, offset, extra)
            except TypeError:
                extra = {}
                req = gifts_request(peer, offset, extra)
            res = await client(req)
        except (TypeError, AttributeError) as e:
            logger.warning("подарки не читаются: %s", e)
            return out
        for sg in res.gifts:
            g = clean(sg, user_id)
            if g:
                out.append(g)
        offset = getattr(res, "next_offset", None)
        if not offset:
            break
    return out


def is_seller(full):
    u = full.users[0]
    if u.bot or u.deleted or u.is_self:
        return False
    fu = full.full_user
    if hasattr(fu, "stargifts_count"):
        cnt = fu.stargifts_count or 0
        if cnt == 0 or cnt > MAX_GIFTS:  # нет подарков — не тратим запрос
            return False
    if getattr(fu, "collector", False):
        return False
    return True


# --- parser ---
async def mark(chat_id, status, error=None):
    await db_exec(
        "UPDATE parse_tasks SET status=?, error=?, updated_at=?"
        " WHERE chat_id=? AND status IN ('pending','running')",
        status, error, int(time.time()), chat_id,
    )


async def candidates(ent, me):
    found = {}

    def add(u):
        if not u or getattr(u, "bot", False) or getattr(u, "deleted", False):
            return
        if u.id == me:
            return
        if ONLY_USERNAME and not getattr(u, "username", None):
            return  # без @ ему не напишешь
        found[u.id] = u

    try:
        async for u in client.iter_participants(ent):
            add(u)
    except RPCError as e:
        logger.warning("участники закрыты: %s", e)
    if found:
        return list(found.values())
    try:
        async for msg in client.iter_messages(ent, limit=5000):
            if msg.sender_id and msg.sender_id > 0:
                add(msg.sender)
    except RPCError as e:
        logger.warning("сообщения не читаются: %s", e)
    return list(found.values())


async def flood_gate():
    wait = flood_until - time.time()
    if wait > 0:
        await asyncio.sleep(wait)


async def warn_owner(chat_id, owner, warned):
    if chat_id in warned:
        return
    warned.append(chat_id)
    try:
        await bot.send_message(owner, "Telegram просит подождать, парсер сам продолжит.")
    except Exception as e:
        logger.warning("не написал владельцу: %s", e)


async def save_user(uid, full, ok):
    u = full.users[0]
    await db_exec(
        "INSERT OR IGNORE INTO users(tg_user_id, username, first_name, rank_ok, created_at)"
        " VALUES (?,?,?,?,?)",
        uid, u.username, u.first_name, 1 if ok else 0, int(time.time()),
    )


async def scan_user(u, chat_id, owner, warned):
    global flood_until
    uid = u.id
    for _ in range(5):
        try:
            await flood_gate()
            if getattr(u, "access_hash", None):
                peer = utils.get_input_peer(u)
            else:
                peer = await client.get_input_entity(uid)
            full = await client(GetFullUserRequest(peer))
            ok = is_seller(full)
            await save_user(uid, full, ok)
            ts = int(time.time())
            if not ok:
                await db_exec(
                    "INSERT OR IGNORE INTO scanned_profiles VALUES (?,?,?)", uid, ts, 0
                )
                return
            gifts = await fetch_gifts(peer, uid)
            uname = full.users[0].username
            profile = f"https://t.me/{uname}" if uname else f"tg://user?id={uid}"
            await db_many(
                "INSERT OR IGNORE INTO nfts(gift_slug, gift_num, name, model_slug,"
                " owner_user_id, owner_username, nft_link, profile_link,"
                " chat_id_source, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                [
                    (
                        g["slug"], g["num"], g["name"], g["slug"], uid, uname,
                        f"https://t.me/nft/{g['slug']}-{g['num']}", profile, chat_id, ts,
                    )
                    for g in gifts
                ],
            )
            await db_exec(
                "INSERT OR IGNORE INTO scanned_profiles VALUES (?,?,?)",
                uid, ts, 1 if gifts else 0,
            )
            return
        except FloodWaitError as e:
            logger.warning("flood %s сек", e.seconds)
            flood_until = max(flood_until, time.time() + e.seconds + random.uniform(1, 3))
            if e.seconds > 60:
                await warn_owner(chat_id, owner, warned)
        except UserPrivacyRestrictedError:
            return
        except RPCError as e:
            logger.warning("профиль %s: %s", uid, e)
            return
        except Exception:
            logger.exception("профиль %s сломался", uid)
            return


async def parse_chat(chat_id):
    try:
        ent = await client.get_entity(chat_id)
        me = (await client.get_me()).id
    except Exception as e:
        logger.warning("чат %s не открылся: %s", chat_id, e)
        await mark(chat_id, "error", str(e)[:200])
        return
    cancelled.discard(chat_id)
    await mark(chat_id, "running")
    row = await db_fetchone("SELECT owner_user_id FROM chats WHERE chat_id=?", chat_id)
    owner = row[0] if row else None
    users = await candidates(ent, me)
    scanned = {r[0] for r in await db_fetch("SELECT user_id FROM scanned_profiles")}
    todo = [u for u in users if u.id not in scanned]
    logger.info("чат %s: кандидатов %s, новых %s", chat_id, len(users), len(todo))
    jobs = asyncio.Queue()
    for u in todo:
        jobs.put_nowait(u)
    warned = []

    async def runner():
        while chat_id not in cancelled:
            try:
                u = jobs.get_nowait()
            except asyncio.QueueEmpty:
                return
            await scan_user(u, chat_id, owner, warned)
            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    await asyncio.gather(*(runner() for _ in range(CONCURRENCY)))
    if chat_id in cancelled:
        cancelled.discard(chat_id)
        logger.info("чат %s убран, парсинг остановлен", chat_id)
        return
    await mark(chat_id, "done")
    await db_exec("UPDATE chats SET status=? WHERE chat_id=?", "done", chat_id)


async def remove_chat(owner, chat_id):
    row = await db_fetchone(
        "SELECT title FROM chats WHERE chat_id=? AND owner_user_id=?", chat_id, owner
    )
    if not row:
        return "Такого чата у тебя нет."
    if await db_fetchone(
        "SELECT 1 FROM parse_tasks WHERE chat_id=? AND status='running'", chat_id
    ):
        cancelled.add(chat_id)
    await db_exec("DELETE FROM chats WHERE chat_id=?", chat_id)
    await db_exec(
        "UPDATE parse_tasks SET status='cancelled', updated_at=?"
        " WHERE chat_id=? AND status IN ('pending','running')",
        int(time.time()), chat_id,
    )
    try:
        await client.delete_dialog(chat_id)
    except Exception as e:
        logger.warning("не вышел из чата %s: %s", chat_id, e)
    return f"Убрал: {html.escape(row[0])}. Слот свободен. Найденные NFT остались в базе."


# --- queue ---
async def worker():
    while True:
        chat_id = await q.get()
        try:
            if await db_fetchone("SELECT 1 FROM chats WHERE chat_id=?", chat_id):
                await parse_chat(chat_id)
        except Exception:
            logger.exception("парсинг чата %s упал", chat_id)
        finally:
            q.task_done()


async def restore_pending():
    rows = await db_fetch(
        "SELECT chat_id FROM parse_tasks WHERE status IN (?,?)", "pending", "running"
    )
    for (chat_id,) in rows:
        q.put_nowait(chat_id)


# --- bot ---
class Throttle(BaseMiddleware):
    def __init__(self):
        self.seen = {}

    async def __call__(self, handler, cb, ctx):
        now = time.monotonic()
        if now - self.seen.get(cb.from_user.id, 0) < 1:
            await cb.answer()
            return None
        self.seen[cb.from_user.id] = now
        return await handler(cb, ctx)


async def safe_edit(msg, text, kb=None):
    try:
        await msg.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        pass


def page_of(page, total, size):
    pages = max(1, -(-total // size))
    return min(max(page, 0), pages - 1), pages


def kb_menu(rows, page, pages):
    kb = InlineKeyboardBuilder()
    for slug, name, n in rows:
        kb.button(text=f"{name} · {n}", callback_data=f"m:{slug}:0")
    kb.adjust(2)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"menu:{page - 1}"))
    nav.append(InlineKeyboardButton(text="🔄 Обновить", callback_data=f"menu:{page}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"menu:{page + 1}"))
    kb.row(*nav)
    return kb.as_markup()


def kb_empty():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить чат", callback_data="add_hint")
    return kb.as_markup()


def kb_model(slug, page, pages):
    kb = InlineKeyboardBuilder()
    nav = []
    if page > 0:
        nav.append(
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"m:{slug}:{page - 1}")
        )
    if page < pages - 1:
        nav.append(
            InlineKeyboardButton(text="➡️ Вперёд", callback_data=f"m:{slug}:{page + 1}")
        )
    if nav:
        kb.row(*nav)
    kb.row(InlineKeyboardButton(text="🏠 Меню", callback_data="home"))
    return kb.as_markup()


async def render_menu(page):
    rows = await db_fetch(
        "SELECT model_slug, MIN(name), COUNT(*) FROM nfts"
        " GROUP BY model_slug ORDER BY MIN(name)"
    )
    if not rows:
        return "Пока пусто. Добавь чат через /add — начну искать.", kb_empty()
    page, pages = page_of(page, len(rows), MENU_SIZE)
    part = rows[page * MENU_SIZE:(page + 1) * MENU_SIZE]
    return "Выбери модель:", kb_menu(part, page, pages)


async def render_model(user_id, slug, page):
    row = await db_fetchone("SELECT COUNT(*) FROM nfts WHERE model_slug=?", slug)
    total = row[0]
    if not total:
        return None, None
    page, pages = page_of(page, total, PAGE_SIZE)
    rows = await db_fetch(
        "SELECT id, name, gift_num, owner_user_id, owner_username, nft_link, profile_link"
        " FROM nfts WHERE model_slug=? ORDER BY created_at DESC, id DESC"
        " LIMIT ? OFFSET ?",
        slug, PAGE_SIZE, page * PAGE_SIZE,
    )
    ts = int(time.time())
    await db_many(
        "INSERT OR IGNORE INTO user_views VALUES (?,?,?)",
        [(user_id, r[0], ts) for r in rows],
    )
    row = await db_fetchone(
        "SELECT COUNT(*) FROM user_views v JOIN nfts n ON n.id = v.nft_id"
        " WHERE v.user_id=? AND n.model_slug=?",
        user_id, slug,
    )
    view = row[0]
    lines = []
    for i, (_, name, n, uid, uname, nft_link, profile) in enumerate(rows):
        who = f"@{html.escape(uname)}" if uname else f"user {uid}"
        lines.append(
            f"{page * PAGE_SIZE + i + 1}. [T] "
            f'<a href="{profile}">{who}</a> - '
            f'<a href="{nft_link}">{html.escape(name)} (\u0023{n})</a>'
        )
    text = (
        f"✅ <b>Результаты ({total})</b>\n"
        f"Страница {page + 1} из {pages}\n\n"
        + "\n".join(lines)
        + f"\n\n🚫 Скрыто ранее просмотренных: {view}"
    )
    return text, kb_model(slug, page, pages)


# --- handlers ---
async def start(msg):
    text, kb = await render_menu(0)
    await msg.answer(text, reply_markup=kb)


async def add(msg, command: CommandObject):
    if not command.args:
        await msg.answer("Кинь ссылку: /add t.me/название — и я зайду в чат.")
        return
    try:
        text = await add_chat(msg.from_user.id, command.args)
    except Exception:
        logger.exception("add_chat упал")
        text = ERR
    await msg.answer(text)


async def info(msg):
    uid = msg.from_user.id
    mine = (await db_fetchone("SELECT COUNT(*) FROM chats WHERE owner_user_id=?", uid))[0]
    chats = (await db_fetchone("SELECT COUNT(*) FROM chats"))[0]
    seen = (await db_fetchone("SELECT COUNT(*) FROM scanned_profiles"))[0]
    with_nft = (
        await db_fetchone("SELECT COUNT(*) FROM scanned_profiles WHERE had_nft=1")
    )[0]
    gifts = (await db_fetchone("SELECT COUNT(*) FROM nfts"))[0]
    models = (await db_fetchone("SELECT COUNT(DISTINCT model_slug) FROM nfts"))[0]
    rows = await db_fetch("SELECT status, COUNT(*) FROM parse_tasks GROUP BY status")
    st = dict(rows)
    await msg.answer(
        "📊 Статистика\n\n"
        f"Твои чаты: {mine} / {MAX_CHATS}\n"
        f"Всего чатов: {num(chats)}\n"
        f"Профилей проверено: {num(seen)}\n"
        f"С NFT: {num(with_nft)}\n"
        f"Уникальных подарков: {num(gifts)}\n"
        f"Моделей: {num(models)}\n\n"
        f"В очереди: {st.get('pending', 0)} · В работе: {st.get('running', 0)}"
        f" · Ошибок: {st.get('error', 0)}"
    )


async def remove(msg):
    rows = await db_fetch(
        "SELECT chat_id, title FROM chats WHERE owner_user_id=?", msg.from_user.id
    )
    if not rows:
        await msg.answer("У тебя нет добавленных чатов.")
        return
    kb = InlineKeyboardBuilder()
    for chat_id, title in rows:
        kb.button(text=f"❌ {(title or 'чат')[:30]}", callback_data=f"rm:{chat_id}")
    kb.adjust(1)
    await msg.answer("Какой чат убрать?", reply_markup=kb.as_markup())


async def remove_cb(cb):
    await cb.answer()
    try:
        chat_id = int(cb.data.split(":", 1)[1])
        text = await remove_chat(cb.from_user.id, chat_id)
    except Exception:
        logger.exception("remove_chat упал")
        text = ERR
    await safe_edit(cb.message, text)


def kb_admin():
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Получить БД", callback_data="adm:get")
    kb.button(text="📤 Загрузить БД", callback_data="adm:put")
    kb.adjust(1)
    return kb.as_markup()


async def admin(msg):
    if msg.from_user.id != ADMIN_ID:
        return
    awaiting_db.discard(msg.from_user.id)
    await msg.answer("Выберите действие:", reply_markup=kb_admin())


async def admin_get_cb(cb):
    await cb.answer()
    if cb.from_user.id != ADMIN_ID:
        return
    path = "export_data.db"
    try:
        if os.path.exists(path):
            os.remove(path)
        await _db.execute(f"VACUUM INTO '{path}'")
        if os.path.getsize(path) > 49 * 1024 * 1024:
            await cb.message.answer("БД больше 49 МБ, Telegram не даст отправить файл.")
            return
        users = (await db_fetchone("SELECT COUNT(*) FROM scanned_profiles"))[0]
        gifts = (await db_fetchone("SELECT COUNT(*) FROM nfts"))[0]
        await cb.message.answer_document(
            FSInputFile(path, filename="data.db"),
            caption=f"Профилей: {num(users)} · NFT: {num(gifts)}",
        )
    except Exception:
        logger.exception("выгрузка БД упала")
        await cb.message.answer(ERR)
    finally:
        if os.path.exists(path):
            os.remove(path)


async def admin_put_cb(cb):
    await cb.answer()
    if cb.from_user.id != ADMIN_ID:
        return
    awaiting_db.add(cb.from_user.id)
    await cb.message.answer(
        "Пришли файл БД (.db) — я добавлю людей и NFT из него к текущей базе, "
        "ничего не удаляя. Отмена: /admin"
    )


async def admin_file(msg):
    uid = msg.from_user.id
    if uid != ADMIN_ID or uid not in awaiting_db:
        return
    path = "import_data.db"
    attached = False
    try:
        await bot.download(msg.document, destination=path)
        with open(path, "rb") as f:
            if f.read(16) != b"SQLite format 3\x00":
                await msg.answer("Это не файл базы SQLite.")
                return
        await _db.execute("ATTACH DATABASE ? AS imp", (path,))
        attached = True
        async with _db.execute(
            "SELECT name FROM imp.sqlite_master WHERE type='table'"
        ) as cur:
            tables = {r[0] for r in await cur.fetchall()}
        if not {"nfts", "scanned_profiles"} <= tables:
            await msg.answer("В этой БД нет нужных таблиц.")
            return
        before_n = (await db_fetchone("SELECT COUNT(*) FROM nfts"))[0]
        before_s = (await db_fetchone("SELECT COUNT(*) FROM scanned_profiles"))[0]
        if "users" in tables:
            await _db.execute(
                "INSERT OR IGNORE INTO users(tg_user_id, username, first_name,"
                " rank_ok, created_at) SELECT tg_user_id, username, first_name,"
                " rank_ok, created_at FROM imp.users"
            )
        await _db.execute(
            "INSERT OR IGNORE INTO scanned_profiles(user_id, scanned_at, had_nft)"
            " SELECT user_id, scanned_at, had_nft FROM imp.scanned_profiles"
        )
        await _db.execute(
            "INSERT OR IGNORE INTO nfts(gift_slug, gift_num, name, model_slug,"
            " owner_user_id, owner_username, nft_link, profile_link,"
            " chat_id_source, created_at) SELECT gift_slug, gift_num, name,"
            " model_slug, owner_user_id, owner_username, nft_link, profile_link,"
            " chat_id_source, created_at FROM imp.nfts"
        )
        await _db.commit()
        after_n = (await db_fetchone("SELECT COUNT(*) FROM nfts"))[0]
        after_s = (await db_fetchone("SELECT COUNT(*) FROM scanned_profiles"))[0]
        awaiting_db.discard(uid)
        await msg.answer(
            "✅ БД загружена\n"
            f"Новых профилей: {num(after_s - before_s)}\n"
            f"Новых NFT: {num(after_n - before_n)}"
        )
    except Exception:
        logger.exception("загрузка БД упала")
        await msg.answer(ERR)
    finally:
        if attached:
            try:
                await _db.execute("DETACH DATABASE imp")
            except Exception:
                logger.exception("detach не удался")
        if os.path.exists(path):
            os.remove(path)


async def help_msg(msg):
    await msg.answer(HELP)


async def home_cb(cb):
    await cb.answer()
    text, kb = await render_menu(0)
    await safe_edit(cb.message, text, kb)


async def menu_cb(cb):
    await cb.answer()
    page = int(cb.data.split(":")[1])
    text, kb = await render_menu(page)
    await safe_edit(cb.message, text, kb)


async def model_cb(cb):
    await cb.answer()
    _, slug, page = cb.data.split(":")
    try:
        text, kb = await render_model(cb.from_user.id, slug, int(page))
    except Exception:
        logger.exception("render_model упал")
        await cb.message.answer(ERR)
        return
    if text is None:
        text, kb = await render_menu(0)
    await safe_edit(cb.message, text, kb)


async def add_hint_cb(cb):
    await cb.answer()
    await cb.message.answer("Кинь ссылку: /add t.me/название — и я зайду в чат.")


def register(dp):
    dp.callback_query.middleware(Throttle())
    dp.message.register(start, CommandStart())
    dp.message.register(add, Command("add"))
    dp.message.register(info, Command("info"))
    dp.message.register(remove, Command("remove"))
    dp.message.register(admin, Command("admin"))
    dp.message.register(admin_file, F.document)
    dp.message.register(help_msg, Command("help"))
    dp.callback_query.register(home_cb, F.data == "home")
    dp.callback_query.register(menu_cb, F.data.startswith("menu:"))
    dp.callback_query.register(model_cb, F.data.startswith("m:"))
    dp.callback_query.register(add_hint_cb, F.data == "add_hint")
    dp.callback_query.register(remove_cb, F.data.startswith("rm:"))
    dp.callback_query.register(admin_get_cb, F.data == "adm:get")
    dp.callback_query.register(admin_put_cb, F.data == "adm:put")


async def set_commands():
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Выбрать модель"),
            BotCommand(command="add", description="Добавить чат"),
            BotCommand(command="info", description="Статистика"),
            BotCommand(command="remove", description="Убрать чат"),
            BotCommand(command="help", description="Как пользоваться"),
        ]
    )
    await bot.set_chat_menu_button(menu_button=MenuButtonCommands())


# --- run ---
async def webhook(req):
    if SECRET and req.headers.get("X-Telegram-Bot-Api-Secret-Token") != SECRET:
        return web.Response(status=403)
    try:
        raw = await req.json()
    except Exception:
        return web.Response(status=400)
    job = asyncio.create_task(dp.feed_raw_update(bot, raw))
    req.app["jobs"].add(job)
    job.add_done_callback(req.app["jobs"].discard)
    return web.Response()


async def health(req):
    return web.json_response({"status": "ok"})


async def keep_alive():
    url = f"https://{HOST}/health"
    await asyncio.sleep(10)
    async with ClientSession() as session:
        while True:
            try:
                async with session.get(url, timeout=ClientTimeout(total=10)) as resp:
                    logger.info("keep-alive: %s", resp.status)
            except Exception as e:
                logger.warning("keep-alive не дошёл: %s", e)
            await asyncio.sleep(PING_EVERY)


async def start_site():
    app = web.Application()
    app["jobs"] = set()
    app.router.add_post(WEBHOOK_PATH, webhook)
    app.router.add_get("/health", health)
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logger.info("сервер на порту %s", PORT)
    return runner


async def main():
    global bot, dp, client
    await init_db()
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML, link_preview_is_disabled=True
        ),
    )
    dp = Dispatcher()
    register(dp)
    runner = await start_site() if HOST else None
    await set_commands()
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        logger.error("SESSION_STRING не рабочий")
        return
    try:
        await client.get_dialogs()
    except Exception as e:
        logger.warning("диалоги не загрузились: %s", e)
    await restore_pending()
    job = asyncio.create_task(worker())
    ping = None
    try:
        if HOST:
            await bot.set_webhook(
                url=f"https://{HOST}{WEBHOOK_PATH}", secret_token=SECRET or None
            )
            logger.info("вебхук стоит: https://%s%s", HOST, WEBHOOK_PATH)
            ping = asyncio.create_task(keep_alive())
            await asyncio.Event().wait()
        else:
            await bot.delete_webhook()
            await dp.start_polling(bot)
    finally:
        job.cancel()
        if ping:
            ping.cancel()
        if runner:
            await runner.cleanup()
        await client.disconnect()
        await bot.session.close()
        if _db:
            await _db.close()


if __name__ == "__main__":
    asyncio.run(main())
