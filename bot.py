# --- imports ---
import asyncio
import html
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone

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
    PeerIdInvalidError,
    RPCError,
    UserIdInvalidError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
    UserPrivacyRestrictedError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetFullChannelRequest, JoinChannelRequest
from telethon.tl.functions.messages import (
    CheckChatInviteRequest,
    ImportChatInviteRequest,
)
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import (
    Channel,
    ChatInviteAlready,
    InputPeerUser,
    InputPeerUserFromMessage,
)

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
SESSIONS = SESSION_STRING.split()  # несколько аккаунтов через пробел
MAX_CHATS = int(os.getenv("MAX_CHATS_PER_USER", 3))
MAX_GIFTS = int(os.getenv("MAX_GIFTS_FOR_SELLER", 50))
DELAY_MIN = float(os.getenv("PARSE_DELAY_MIN", 0.2))
DELAY_MAX = float(os.getenv("PARSE_DELAY_MAX", 0.6))
CONCURRENCY = int(os.getenv("PARSE_CONCURRENCY", 8))
ADMIN_ID = int(os.getenv("ADMIN_ID", 6811074441))
MSG_CAP = int(os.getenv("MAX_SCAN_MESSAGES", 30000))
MSG_TARGET = int(os.getenv("MSG_TARGET", 1000))
RECHECK_MAX_MIN = int(os.getenv("RECHECK_MAX_MINUTES", 120))
RECHECK_HOUR = int(os.getenv("RECHECK_HOUR_MSK", 4))
MSK = timezone(timedelta(hours=3))
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
    "4. /remove — убрать свой чат (освобождает слот).\n"
    "5. /sl — скам-база, тем, кому писать не надо.\n\n"
    "Приватные чаты с одобрением заявки не поддерживаются.\n"
    "Профиль проверяется один раз, повторно не сканируется."
)

bot = None
dp = None
q = asyncio.Queue()
_db = None
accs = []
peer_cache = {}
biz_owner = {}
running = asyncio.Event()
active = 0
cancelled = set()
awaiting_db = set()
slow = 0.0
conc = [CONCURRENCY]

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
    "CREATE TABLE IF NOT EXISTS peers ( "
    "    acc INTEGER, "
    "    user_id INTEGER, "
    "    access_hash INTEGER, "
    "    PRIMARY KEY (acc, user_id) "
    "); "
    "CREATE TABLE IF NOT EXISTS biz_conn ( "
    "    connection_id TEXT PRIMARY KEY, "
    "    user_id INTEGER, "
    "    user_chat_id INTEGER, "
    "    enabled INTEGER "
    "); "
    "CREATE TABLE IF NOT EXISTS biz_models ( "
    "    user_id INTEGER, "
    "    num INTEGER, "
    "    slug TEXT, "
    "    name TEXT, "
    "    PRIMARY KEY (user_id, num) "
    "); "
    "CREATE TABLE IF NOT EXISTS biz_pages ( "
    "    user_id INTEGER, "
    "    model_slug TEXT, "
    "    page INTEGER, "
    "    PRIMARY KEY (user_id, model_slug) "
    "); "
    "CREATE TABLE IF NOT EXISTS scams ( "
    "    tg_user_id INTEGER PRIMARY KEY, "
    "    username TEXT, "
    "    reason TEXT, "
    "    added_by INTEGER, "
    "    added_at TEXT "
    "); "
    "CREATE INDEX IF NOT EXISTS ix_scams_added ON scams(added_at DESC); "
    "CREATE INDEX IF NOT EXISTS idx_nfts_model ON nfts(model_slug); "
    "CREATE INDEX IF NOT EXISTS idx_nfts_owner ON nfts(owner_user_id); "
    "CREATE INDEX IF NOT EXISTS idx_views_user ON user_views(user_id); "
)


UPSERT_NFT = (
    "INSERT INTO nfts(gift_slug, gift_num, name, model_slug, owner_user_id,"
    " owner_username, nft_link, profile_link, chat_id_source, created_at)"
    " VALUES (?,?,?,?,?,?,?,?,?,?)"
    " ON CONFLICT(gift_slug, gift_num) DO UPDATE SET"
    " owner_user_id=excluded.owner_user_id, owner_username=excluded.owner_username,"
    " profile_link=excluded.profile_link, name=excluded.name"
)


NOT_SCAM = "owner_user_id NOT IN (SELECT tg_user_id FROM scams)"


async def init_db():
    global _db
    _db = await aiosqlite.connect(DB)
    await _db.execute("PRAGMA journal_mode=WAL")
    await _db.execute("PRAGMA synchronous=NORMAL")
    await _db.executescript(SCHEMA)
    for col in ("premium", "level"):
        try:
            await _db.execute(f"ALTER TABLE users ADD COLUMN {col} INTEGER")
        except Exception:
            pass
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


class Acc:
    def __init__(self, idx, client):
        self.idx = idx  # номер в SESSION_STRING (с 0), по нему хранятся access_hash
        self.client = client
        self.me = None
        self.flood_until = 0.0
        self.sem = asyncio.Semaphore(CONCURRENCY)


async def get_acc(pref=0):
    """Первый свободный аккаунт по кругу, начиная с pref."""
    while True:
        now = time.time()
        n = len(accs)
        for k in range(n):
            a = accs[(pref + k) % n]
            if a.flood_until <= now:
                return a
        await asyncio.sleep(max(min(a.flood_until for a in accs) - now, 0.5))


def mark_flood(acc, seconds):
    acc.flood_until = max(acc.flood_until, time.time() + seconds + random.uniform(1, 3))
    logger.warning("аккаунт %s: flood %s сек, переключаюсь", acc.idx + 1, seconds)


def remember(acc, uid, full):
    try:
        p = utils.get_input_peer(full.users[0])
        peer_cache[(acc.idx, uid)] = p
        return p
    except Exception:
        return None


async def resolve_peer(acc, uid, username=None, u=None, msg=None, home=None, chat_id=None):
    c = acc.client
    p = peer_cache.get((acc.idx, uid))
    if p:
        return p
    row = await db_fetchone(
        "SELECT access_hash FROM peers WHERE acc=? AND user_id=?", acc.idx, uid
    )
    if row:
        p = InputPeerUser(uid, row[0])
        peer_cache[(acc.idx, uid)] = p
        return p
    if (
        u is not None and home == acc.idx and not getattr(u, "min", False)
        and getattr(u, "access_hash", None)
    ):
        return utils.get_input_peer(u)
    if msg is not None and chat_id is not None:
        try:
            if home == acc.idx:
                p = await msg.get_input_sender()
                if p:
                    return p
            else:
                chat = await c.get_input_entity(chat_id)
                return InputPeerUserFromMessage(chat, msg.id, uid)
        except Exception as e:
            logger.debug("peer из сообщения: %s", e)
    if username:
        return await c.get_input_entity(username)
    return await c.get_input_entity(uid)


async def join(c, link):
    kind, key = link
    if kind == "public":
        ent = await c.get_entity(key)
        await c(JoinChannelRequest(ent))
        return ent
    info = await c(CheckChatInviteRequest(key))
    if isinstance(info, ChatInviteAlready):
        return info.chat
    res = await c(ImportChatInviteRequest(key))
    return res.chats[0]


async def add_chat(owner, text):
    link = parse_link(text)
    if not link:
        return BAD_LINK
    ent, first_err = None, None
    for a in accs:  # заходим всеми аккаунтами
        try:
            e = await join(a.client, link)
            ent = ent or e
        except (InviteRequestSentError, FloodWaitError, RPCError, ValueError, TypeError) as ex:
            logger.warning("аккаунт %s не зашёл: %s", a.idx + 1, ex)
            first_err = first_err or ex
    if ent is None:
        if isinstance(first_err, InviteRequestSentError):
            return "Не зашёл: там одобрение заявки. Такие чаты не парсятся."
        if isinstance(first_err, FloodWaitError):
            return ERR
        return BAD_LINK
    title = getattr(ent, "title", None)
    if not title:
        return BAD_LINK
    chat_id = utils.get_peer_id(ent)
    ts = int(time.time())
    existing = await db_fetchone("SELECT owner_user_id FROM chats WHERE chat_id=?", chat_id)
    if existing:
        if existing[0] != owner:
            return "Этот чат уже в списке."
        if await db_fetchone(
            "SELECT 1 FROM parse_tasks WHERE chat_id=? AND status IN ('pending','running')",
            chat_id,
        ):
            return "Этот чат сейчас сканируется, подожди."
        await db_exec("UPDATE chats SET status='queued' WHERE chat_id=?", chat_id)
        await db_exec(
            "INSERT INTO parse_tasks(chat_id, status, created_at, updated_at)"
            " VALUES (?,?,?,?)",
            chat_id, "pending", ts, ts,
        )
        await q.put(chat_id)
        return (
            f"Чат уже был в списке: {html.escape(title)}. "
            "Запустил сканирование заново, возьму новых людей."
        )
    cnt = await db_fetchone("SELECT COUNT(*) FROM chats WHERE owner_user_id=?", owner)
    if cnt[0] >= MAX_CHATS:
        for a in accs:  # не оставляем аккаунты в лишнем чате
            try:
                await a.client.delete_dialog(chat_id)
            except Exception as e:
                logger.warning("аккаунт %s не вышел из чата: %s", a.idx + 1, e)
        return f"У тебя уже {MAX_CHATS} чата — это максимум. Больше добавить нельзя."
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


async def fetch_gifts(c, peer, user_id, strict=False):
    if not (GetSavedStarGiftsRequest or GetUserGiftsRequest):
        logger.warning("в этой версии Telethon нет запроса подарков")
        if strict:
            raise RuntimeError("нет запроса подарков")
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
            res = await c(req)
        except (TypeError, AttributeError) as e:
            logger.warning("подарки не читаются: %s", e)
            if strict:
                raise RuntimeError("подарки не читаются")
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


async def produce(chat_id, scanned, jobs, stats):
    """Кладёт людей в очередь по мере нахождения, воркеры сканируют сразу."""
    sent = set()
    my_ids = {a.me for a in accs}
    try:
        acc = await get_acc(0)
        ent = await acc.client.get_entity(chat_id)

        async def rotate(seconds):
            nonlocal acc, ent
            mark_flood(acc, seconds)
            acc = await get_acc(0)
            ent = await acc.client.get_entity(chat_id)

        def push(u, msg=None):
            if not u or getattr(u, "bot", False) or getattr(u, "deleted", False):
                return
            if u.id in my_ids or u.id in scanned or u.id in sent:
                return
            if ONLY_USERNAME and not getattr(u, "username", None):
                return  # без @ ему не напишешь
            sent.add(u.id)
            jobs.put_nowait((u, msg, acc.idx))

        total, hidden_flag = None, False
        if isinstance(ent, Channel):
            try:
                fc = (await acc.client(GetFullChannelRequest(ent))).full_chat
                total = getattr(fc, "participants_count", None)
                hidden_flag = bool(getattr(fc, "participants_hidden", False))
            except Exception as e:
                logger.warning("не узнал данные чата: %s", e)
        if not total:
            try:
                total = (await acc.client.get_participants(ent, limit=0)).total
            except Exception as e:
                logger.warning("не узнал число участников: %s", e)
        seen = 0
        for _ in range(4):
            try:
                seen = 0
                async for u in acc.client.iter_participants(ent):
                    if chat_id in cancelled:
                        return
                    seen += 1
                    push(u)
                break
            except FloodWaitError as e:
                await rotate(e.seconds)
            except RPCError as e:
                logger.warning("участники закрыты: %s", e)
                break
        stats["seen"], stats["total"] = seen, total
        # видны только админы (или никого) -> берём авторов сообщений
        if not (hidden_flag or seen == 0 or (total and seen < total * 0.5)):
            return
        stats["hidden"] = True
        target = MSG_TARGET if not total else min(MSG_TARGET, max(1, total // 2))
        logger.info("чат %s: список скрыт (%s из %s), цель %s", chat_id, seen, total, target)
        last_id, msgs = 0, 0
        for _ in range(6):
            try:
                async for msg in acc.client.iter_messages(
                    ent, limit=MSG_CAP, offset_id=last_id, wait_time=0
                ):
                    if chat_id in cancelled:
                        return
                    last_id = msg.id
                    msgs += 1
                    sid = msg.sender_id
                    if not sid or sid < 0 or sid in sent or sid in scanned or sid in my_ids:
                        continue
                    u = msg.sender
                    if u is None:
                        try:
                            u = await msg.get_sender()
                        except Exception:
                            u = None
                    push(u, msg)
                    if len(sent) >= target:
                        break
                break
            except FloodWaitError as e:
                await rotate(e.seconds)
            except RPCError as e:
                logger.warning("сообщения не читаются: %s", e)
                break
        stats["msgs"] = msgs
    except Exception:
        logger.exception("сбор людей из чата %s упал", chat_id)
    finally:
        for _ in range(CONCURRENCY * max(1, len(accs))):
            jobs.put_nowait(None)


async def warn_owner(chat_id, owner, warned):
    if chat_id in warned:
        return
    warned.append(chat_id)
    try:
        await bot.send_message(owner, "Telegram просит подождать, парсер сам продолжит.")
    except Exception as e:
        logger.warning("не написал владельцу: %s", e)


def stars_level(full):
    r = getattr(full.full_user, "stars_rating", None)
    lv = getattr(r, "level", None)
    return lv if isinstance(lv, int) else None


async def save_user(uid, full, ok):
    u = full.users[0]
    await db_exec(
        "INSERT INTO users(tg_user_id, username, first_name, rank_ok, created_at,"
        " premium, level) VALUES (?,?,?,?,?,?,?)"
        " ON CONFLICT(tg_user_id) DO UPDATE SET"
        " username=excluded.username, first_name=excluded.first_name,"
        " rank_ok=excluded.rank_ok, premium=excluded.premium, level=excluded.level",
        uid, u.username, u.first_name, 1 if ok else 0, int(time.time()),
        1 if getattr(u, "premium", False) else 0, stars_level(full),
    )


async def scan_user(u, msg, home, chat_id, owner, warned, stats, pref):
    global slow
    uid = u.id
    if await is_scammer(uid):
        return
    for _ in range(8):
        acc = await get_acc(pref)
        try:
            async with acc.sem:
                c = acc.client
                try:
                    peer = await resolve_peer(acc, uid, u.username, u, msg, home, chat_id)
                    full = await c(GetFullUserRequest(peer))
                except (PeerIdInvalidError, UserIdInvalidError, ValueError):
                    if not getattr(u, "username", None):
                        raise
                    peer = await c.get_input_entity(u.username)
                    full = await c(GetFullUserRequest(peer))
                peer = remember(acc, uid, full) or peer
                ok = is_seller(full)
                await save_user(uid, full, ok)
                ts = int(time.time())
                if not ok:
                    await db_exec(
                        "INSERT OR IGNORE INTO scanned_profiles VALUES (?,?,?)", uid, ts, 0
                    )
                    stats["done"] += 1
                    return
                if isinstance(peer, InputPeerUser):
                    await db_exec(
                        "INSERT OR REPLACE INTO peers VALUES (?,?,?)",
                        acc.idx, uid, peer.access_hash,
                    )
                gifts = await fetch_gifts(c, peer, uid)
                uname = full.users[0].username
                profile = f"https://t.me/{uname}" if uname else f"tg://user?id={uid}"
                await db_many(
                    UPSERT_NFT,
                    [
                        (
                            g["slug"], g["num"], g["name"], g["slug"], uid, uname,
                            f"https://t.me/nft/{g['slug']}-{g['num']}", profile, chat_id, ts,
                        )
                        for g in gifts
                    ],
                )
                await db_exec(
                    "INSERT OR REPLACE INTO scanned_profiles VALUES (?,?,?)",
                    uid, ts, 1 if gifts else 0,
                )
                stats["done"] += 1
                if gifts:
                    stats["nft"] += 1
                return
        except FloodWaitError as e:
            mark_flood(acc, e.seconds)
            slow = min(slow + 0.3, 3.0)  # после flood идём чуть медленнее
            if e.seconds > 60 and all(a.flood_until > time.time() for a in accs):
                await warn_owner(chat_id, owner, warned)
        except UserPrivacyRestrictedError:
            await db_exec(
                "INSERT OR IGNORE INTO scanned_profiles VALUES (?,?,?)",
                uid, int(time.time()), 0,
            )
            stats["done"] += 1
            return
        except RPCError as e:
            logger.warning("профиль %s: %s", uid, e)
            stats["err"] += 1
            return
        except Exception:
            logger.exception("профиль %s сломался", uid)
            stats["err"] += 1
            return
    stats["err"] += 1


async def notify(user_id, text):
    if not user_id:
        return
    try:
        await bot.send_message(user_id, text)
    except Exception as e:
        logger.warning("не написал владельцу: %s", e)


async def parse_chat(chat_id):
    row = await db_fetchone(
        "SELECT owner_user_id, title FROM chats WHERE chat_id=?", chat_id
    )
    owner = row[0] if row else None
    title = html.escape(row[1] or "чат") if row else "чат"
    try:
        acc0 = await get_acc(0)
        await acc0.client.get_entity(chat_id)
    except Exception as e:
        logger.warning("чат %s не открылся: %s", chat_id, e)
        await mark(chat_id, "error", str(e)[:200])
        await notify(owner, f"Не открыл чат «{title}»: {html.escape(str(e)[:150])}")
        return
    cancelled.discard(chat_id)
    await mark(chat_id, "running")
    scanned = {r[0] for r in await db_fetch("SELECT user_id FROM scanned_profiles")}
    scanned |= {r[0] for r in await db_fetch("SELECT tg_user_id FROM scams")}
    jobs = asyncio.Queue()
    stats = {"done": 0, "nft": 0, "err": 0, "hidden": False,
             "seen": 0, "total": None, "msgs": 0}
    warned = []

    async def worker_loop(k):
        global active
        pref = k % len(accs)
        while True:
            item = await jobs.get()
            if item is None or chat_id in cancelled:
                return
            await running.wait()  # ночью тут пауза
            u, msg, home = item
            active += 1
            try:
                await scan_user(u, msg, home, chat_id, owner, warned, stats, pref)
            finally:
                active -= 1
            n = stats["done"] + stats["err"]
            if n % 100 == 0:
                logger.info("чат %s: проверено %s", chat_id, n)
            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX) + slow)

    olds = [a.client.flood_sleep_threshold for a in accs]
    for a in accs:
        a.client.flood_sleep_threshold = 20  # длинные flood идут в переключение
    try:
        prod = asyncio.create_task(produce(chat_id, scanned, jobs, stats))
        await asyncio.gather(*(worker_loop(k) for k in range(CONCURRENCY * len(accs))))
        await prod
    finally:
        for a, old in zip(accs, olds):
            a.client.flood_sleep_threshold = old
    if chat_id in cancelled:
        cancelled.discard(chat_id)
        logger.info("чат %s убран, парсинг остановлен", chat_id)
        return
    await mark(chat_id, "done")
    await db_exec("UPDATE chats SET status=? WHERE chat_id=?", "done", chat_id)
    logger.info("чат %s готов: %s", chat_id, stats)
    lines = [
        f"✅ Чат «{title}» просканирован",
        f"Проверено профилей: {num(stats['done'])}",
        f"С NFT: {num(stats['nft'])}",
    ]
    if stats["err"]:
        lines.append(f"Ошибок: {num(stats['err'])}")
    if stats["hidden"]:
        lines.append(
            f"Список участников скрыт — брал авторов сообщений (прочитано {num(stats['msgs'])})."
        )
    if stats["done"] + stats["err"] == 0:
        lines.append("Новых профилей не нашлось: все уже проверены или нет людей с @username.")
    await notify(owner, "\n".join(lines))


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
    for a in accs:
        try:
            await a.client.delete_dialog(chat_id)
        except Exception as e:
            logger.warning("аккаунт %s не вышел из чата %s: %s", a.idx + 1, chat_id, e)
    return f"Убрал: {html.escape(row[0])}. Слот свободен. Найденные NFT остались в базе."


# --- nightly recheck ---
async def sync_user_nfts(uid, uname, gifts, stats):
    ts = int(time.time())
    existing = await db_fetch(
        "SELECT id, gift_slug, gift_num FROM nfts WHERE owner_user_id=?", uid
    )
    have = {(sl, n): i for i, sl, n in existing}
    now = {(g["slug"], g["num"]): g for g in gifts}
    gone = [i for k, i in have.items() if k not in now]
    if gone:
        await db_many("DELETE FROM user_views WHERE nft_id=?", [(i,) for i in gone])
        await db_many("DELETE FROM nfts WHERE id=?", [(i,) for i in gone])
        stats["removed"] += len(gone)
    new = [g for k, g in now.items() if k not in have]
    profile = f"https://t.me/{uname}" if uname else f"tg://user?id={uid}"
    if new:
        await db_many(
            UPSERT_NFT,
            [
                (
                    g["slug"], g["num"], g["name"], g["slug"], uid, uname,
                    f"https://t.me/nft/{g['slug']}-{g['num']}", profile, None, ts,
                )
                for g in new
            ],
        )
        stats["added"] += len(new)
    if uname and have:
        await db_exec(
            "UPDATE nfts SET owner_username=?, profile_link=? WHERE owner_user_id=?",
            uname, profile, uid,
        )
    await db_exec(
        "INSERT OR REPLACE INTO scanned_profiles VALUES (?,?,?)",
        uid, ts, 1 if now else 0,
    )


async def recheck_user(uid, username, stats, pref):
    global slow
    for _ in range(8):
        acc = await get_acc(pref)
        try:
            async with acc.sem:
                c = acc.client
                peer = await resolve_peer(acc, uid, username)
                full = await c(GetFullUserRequest(peer))
                peer = remember(acc, uid, full) or peer
                u = full.users[0]
                await save_user(uid, full, is_seller(full))
                if u.deleted:
                    gifts = []
                elif is_seller(full):
                    gifts = await fetch_gifts(c, peer, uid, strict=True)
                else:
                    gifts = []  # подарков не видно или стал коллекционером
                await sync_user_nfts(uid, u.username, gifts, stats)
                if gifts and isinstance(peer, InputPeerUser):
                    await db_exec(
                        "INSERT OR REPLACE INTO peers VALUES (?,?,?)",
                        acc.idx, uid, peer.access_hash,
                    )
                stats["checked"] += 1
                return
        except FloodWaitError as e:
            mark_flood(acc, e.seconds)
            slow = min(slow + 0.3, 3.0)
        except (
            PeerIdInvalidError, UserIdInvalidError, UsernameNotOccupiedError,
            UsernameInvalidError, ValueError,
        ):
            stats["skipped"] += 1  # не нашли человека — ничего не удаляем
            return
        except UserPrivacyRestrictedError:
            stats["skipped"] += 1
            return
        except (RPCError, RuntimeError) as e:
            logger.warning("перепроверка %s: %s", uid, e)
            stats["err"] += 1
            return
        except Exception:
            logger.exception("перепроверка %s сломалась", uid)
            stats["err"] += 1
            return
    stats["err"] += 1


async def run_recheck():
    stats = {"checked": 0, "removed": 0, "added": 0, "skipped": 0, "err": 0}
    running.clear()  # парсинг встаёт на паузу
    left = 0
    try:
        for _ in range(120):  # ждём, пока дойдут текущие сканы
            if active == 0:
                break
            await asyncio.sleep(1)
        owners = await db_fetch(
            "SELECT owner_user_id, MAX(owner_username) FROM nfts"
            f" WHERE {NOT_SCAM} GROUP BY owner_user_id"
        )
        first = {r[0] for r in owners}
        rest = await db_fetch(
            "SELECT sp.user_id, u.username FROM scanned_profiles sp"
            " LEFT JOIN users u ON u.tg_user_id = sp.user_id"
            " WHERE sp.user_id NOT IN (SELECT tg_user_id FROM scams)"
            " ORDER BY sp.had_nft DESC, sp.scanned_at ASC"
        )
        jobs = asyncio.Queue()
        for r in owners:
            jobs.put_nowait(r)
        for r in rest:
            if r[0] not in first:
                jobs.put_nowait(r)
        logger.info("ночная перепроверка: %s профилей", jobs.qsize())
        deadline = time.time() + RECHECK_MAX_MIN * 60

        async def w(k):
            pref = k % len(accs)
            while time.time() < deadline:
                try:
                    uid, uname = jobs.get_nowait()
                except asyncio.QueueEmpty:
                    return
                await recheck_user(uid, uname, stats, pref)
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX) + slow)

        await asyncio.gather(*(w(k) for k in range(CONCURRENCY * len(accs))))
        left = jobs.qsize()
        await db_exec("DELETE FROM user_views WHERE nft_id NOT IN (SELECT id FROM nfts)")
    finally:
        running.set()
    logger.info("перепроверка готова: %s", stats)
    text = (
        "🌙 Ночная перепроверка завершена\n"
        f"Проверено профилей: {num(stats['checked'])}\n"
        f"NFT убрано: {num(stats['removed'])}\n"
        f"NFT добавлено: {num(stats['added'])}\n"
        f"Не нашёл профиль: {num(stats['skipped'])} · Ошибок: {num(stats['err'])}"
    )
    if left:
        text += f"\nНе хватило времени, осталось: {num(left)} (продолжу в следующий раз)"
    text += "\nПарсинг продолжается."
    await notify(ADMIN_ID, text)


async def nightly():
    while True:
        now = datetime.now(MSK)
        nxt = now.replace(hour=RECHECK_HOUR, minute=0, second=0, microsecond=0)
        if nxt <= now:
            nxt += timedelta(days=1)
        await asyncio.sleep((nxt - now).total_seconds())
        try:
            await run_recheck()
        except Exception:
            logger.exception("ночная перепроверка упала")
            running.set()
        await asyncio.sleep(90)


# --- queue ---
async def worker():
    while True:
        chat_id = await q.get()
        try:
            if await db_fetchone("SELECT 1 FROM chats WHERE chat_id=?", chat_id):
                await parse_chat(chat_id)
        except Exception:
            logger.exception("парсинг чата %s упал", chat_id)
            await mark(chat_id, "error", "crash")
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


def kb_menu(rows, page, pages, uid):
    kb = InlineKeyboardBuilder()
    for slug, name, n in rows:
        kb.button(text=f"{name} · {n}", callback_data=f"m:{slug}:0")
    kb.adjust(2)
    nav = []
    if page > 0:
        nav.append(
            InlineKeyboardButton(
                text="⬅️", callback_data=f"menu:{page - 1}", style="primary"
            )
        )
    nav.append(
        InlineKeyboardButton(
            text="🔄 Обновить", callback_data=f"menu:{page}", style="success"
        )
    )
    if page < pages - 1:
        nav.append(
            InlineKeyboardButton(
                text="➡️", callback_data=f"menu:{page + 1}", style="primary"
            )
        )
    kb.row(*nav)
    add_filters(kb, uid, f"s:{page}")
    return kb.as_markup()


def kb_empty():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить чат", callback_data="add_hint")
    return kb.as_markup()


def kb_model(slug, page, pages, uid):
    kb = InlineKeyboardBuilder()
    nav = []
    if page > 0:
        nav.append(
            InlineKeyboardButton(
                text="⬅️ Назад", callback_data=f"m:{slug}:{page - 1}", style="primary"
            )
        )
    if page < pages - 1:
        nav.append(
            InlineKeyboardButton(
                text="➡️ Вперёд", callback_data=f"m:{slug}:{page + 1}", style="primary"
            )
        )
    if nav:
        kb.row(*nav)
    add_filters(kb, uid, f"m:{slug}:{page}")
    kb.row(InlineKeyboardButton(text="🏠 Меню", callback_data="home", style="primary"))
    return kb.as_markup()


async def render_menu(page, uid, note=None):
    fs, fa = fcond(uid)
    rows = await db_fetch(
        "SELECT model_slug, MIN(name), COUNT(DISTINCT owner_user_id) FROM nfts"
        f" WHERE {NOT_SCAM}{fs} GROUP BY model_slug ORDER BY MIN(name)",
        *fa,
    )
    if not rows:
        if fa:
            return NOTHING, kb_nothing("s:0")
        return "Пока пусто. Добавь чат через /add — начну искать.", kb_empty()
    page, pages = page_of(page, len(rows), MENU_SIZE)
    part = rows[page * MENU_SIZE:(page + 1) * MENU_SIZE]
    head = fhead(uid, note)
    text = "Выбери модель:" + ("\n" + head if head else "")
    return text, kb_menu(part, page, pages, uid)


async def render_model(user_id, slug, page, note=None):
    fs, fa = fcond(user_id)
    row = await db_fetchone(
        "SELECT COUNT(DISTINCT owner_user_id) FROM nfts"
        f" WHERE model_slug=? AND {NOT_SCAM}{fs}", slug, *fa
    )
    total = row[0]
    if not total:
        if fa:
            return NOTHING, kb_nothing(f"m:{slug}:0")
        return None, None
    page, pages = page_of(page, total, PAGE_SIZE)
    rows = await db_fetch(
        "SELECT id, name, gift_num, owner_user_id, owner_username, nft_link, profile_link"
        f" FROM nfts WHERE model_slug=? AND {NOT_SCAM}{fs}"
        " GROUP BY owner_user_id HAVING id = MAX(id)"
        " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
        slug, *fa, PAGE_SIZE, page * PAGE_SIZE,
    )
    ts = int(time.time())
    await db_many(
        "INSERT OR IGNORE INTO user_views VALUES (?,?,?)",
        [(user_id, r[0], ts) for r in rows],
    )
    row = await db_fetchone(
        "SELECT COUNT(DISTINCT n.owner_user_id) FROM user_views v JOIN nfts n ON n.id = v.nft_id"
        f" WHERE v.user_id=? AND n.model_slug=? AND n.{NOT_SCAM}{fcond(user_id, 'n.owner_user_id')[0]}",
        user_id, slug, *fa,
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
    head = fhead(user_id, note)
    text = (
        f"✅ <b>Результаты ({total})</b>\n"
        f"Страница {page + 1} из {pages}\n"
        + (head + "\n" if head else "")
        + "\n"
        + "\n".join(lines)
        + f"\n\n🚫 Скрыто ранее просмотренных: {view}"
    )
    return text, kb_model(slug, page, pages, user_id)


# --- filters ---
filt = {}
back_screen = {}
NOTHING = 'По этим фильтрам ничего. Попробуй "Все" или сбрось фильтры.'
LEVEL_HINT = "(фильтр по уровню недоступен для части профилей)"
PREM = {None: "все", True: "да", False: "нет"}


def fstate(uid):
    return filt.setdefault(
        uid, {"level": None, "premium": None, "a_level": None, "a_premium": None}
    )


def fcond(uid, col="owner_user_id"):
    st = filt.get(uid)
    sql, args = "", []
    if not st:
        return sql, args
    if st["a_premium"] is not None:
        sql += f" AND {col} IN (SELECT tg_user_id FROM users WHERE premium=?)"
        args.append(1 if st["a_premium"] else 0)
    if st["a_level"] is not None:
        sql += (
            f" AND ({col} IN (SELECT tg_user_id FROM users WHERE level=?)"
            f" OR {col} NOT IN (SELECT tg_user_id FROM users WHERE level IS NOT NULL))"
        )
        args.append(st["a_level"])
    return sql, args


def fany(st):
    return bool(st) and any(v is not None for v in st.values())


def fhead(uid, note=None):
    st = filt.get(uid)
    lines = [note] if note else []
    if fany(st):
        lv = st["level"] if st["level"] is not None else "все"
        line = f"Фильтры: уровень {lv} · premium {PREM[st['premium']]}"
        if (st["level"], st["premium"]) != (st["a_level"], st["a_premium"]):
            line += " · <i>не применено</i>"
        lines.append(line)
        if st["level"] is not None:
            lines.append(LEVEL_HINT)
    return "\n".join(lines)


def add_filters(kb, uid, scr):
    st = filt.get(uid) or {"level": None, "premium": None}
    lv = st["level"] if st["level"] is not None else "все"
    kb.row(InlineKeyboardButton(text="Фильтры:", callback_data="f:noop"))
    kb.row(
        InlineKeyboardButton(text=f"🎖 Уровень: {lv}", callback_data=f"f:l:{scr}"),
        InlineKeyboardButton(
            text=f"⭐ Premium: {PREM[st['premium']]}", callback_data=f"f:p:{scr}"
        ),
    )
    row = [
        InlineKeyboardButton(
            text="✅ Продолжить", callback_data=f"f:ok:{scr}", style="success"
        )
    ]
    if fany(filt.get(uid)):
        row.append(InlineKeyboardButton(text="♻️ Сбросить", callback_data=f"f:rs:{scr}"))
    kb.row(*row)


def kb_nothing(scr):
    kb = InlineKeyboardBuilder()
    kb.button(text="♻️ Сбросить фильтры", callback_data=f"f:rs:{scr}", style="success")
    kb.button(text="🏠 Меню", callback_data="home", style="primary")
    kb.adjust(1)
    return kb.as_markup()


def kb_level():
    kb = InlineKeyboardBuilder()
    for n in range(1, 11):
        kb.button(text=str(n), callback_data=f"f:s:{n}")
    kb.button(text="Все уровни", callback_data="f:s:0")
    kb.button(text="⬅️ Назад", callback_data="f:b")
    kb.adjust(5, 5, 1, 1)
    return kb.as_markup()


def first_page(scr):
    kind, _, rest = scr.partition(":")
    if kind == "m":
        return f"m:{rest.rpartition(':')[0]}:0"
    return "s:0"


async def render_scr(uid, scr, note=None):
    kind, _, rest = scr.partition(":")
    if kind == "m":
        slug, _, page = rest.rpartition(":")
        text, kb = await render_model(uid, slug, int(page), note)
        if text is not None:
            return text, kb
        return await render_menu(0, uid, note)
    return await render_menu(int(rest), uid, note)


async def filt_cb(cb):
    await cb.answer()
    uid = cb.from_user.id
    _, act, *rest = cb.data.split(":", 2)
    arg = rest[0] if rest else ""
    st = fstate(uid)
    try:
        if act == "l":
            back_screen[uid] = arg
            await safe_edit(
                cb.message,
                'Выбери уровень Telegram.\nЕсли не уверен — оставь "все".',
                kb_level(),
            )
            return
        if act == "s":
            st["level"] = int(arg) or None
            text, kb = await render_scr(uid, back_screen.get(uid, "s:0"))
        elif act == "b":
            text, kb = await render_scr(uid, back_screen.get(uid, "s:0"))
        elif act == "p":
            st["premium"] = {None: True, True: False, False: None}[st["premium"]]
            text, kb = await render_scr(uid, arg)
        elif act == "rs":
            st.update(level=None, premium=None, a_level=None, a_premium=None)
            text, kb = await render_scr(uid, first_page(arg), "Фильтры сброшены.")
        elif act == "ok":
            st["a_level"], st["a_premium"] = st["level"], st["premium"]
            await safe_edit(cb.message, "Загружаю результаты…")
            await asyncio.sleep(random.uniform(0.3, 0.6))
            text, kb = await render_scr(uid, first_page(arg))
        else:
            return
    except Exception:
        logger.exception("filt_cb упал")
        await cb.message.answer(ERR)
        return
    await safe_edit(cb.message, text, kb)


# --- scams ---
async def add_scam(uid, username, reason, admin_id):
    if not username:
        row = await db_fetchone("SELECT username FROM users WHERE tg_user_id=?", uid)
        username = row[0] if row else None
    await db_exec(
        "INSERT INTO scams(tg_user_id, username, reason, added_by, added_at)"
        " VALUES (?,?,?,?,?) ON CONFLICT(tg_user_id) DO UPDATE SET"
        " username=excluded.username, reason=COALESCE(excluded.reason, scams.reason),"
        " added_by=excluded.added_by, added_at=excluded.added_at",
        uid, username, reason, admin_id, datetime.now(MSK).isoformat(timespec="seconds"),
    )


async def is_scammer(uid):
    return bool(await db_fetchone("SELECT 1 FROM scams WHERE tg_user_id=?", uid))


def kb_scams(page, pages, rows=None):
    kb = InlineKeyboardBuilder()
    if rows:
        for i, (uid, uname, _) in enumerate(rows, page * PAGE_SIZE + 1):
            who = f"@{uname}" if uname else str(uid)
            kb.button(
                text=f"❌ {i}. {who}"[:40],
                callback_data=f"sc:del:{uid}:{page}",
                style="danger",
            )
        kb.adjust(2)
    nav = []
    if page > 0:
        nav.append(
            InlineKeyboardButton(text="⬅️", callback_data=f"sc:{page - 1}", style="primary")
        )
    if page < pages - 1:
        nav.append(
            InlineKeyboardButton(text="➡️", callback_data=f"sc:{page + 1}", style="primary")
        )
    nav.append(InlineKeyboardButton(text="🏠 Меню", callback_data="home", style="primary"))
    kb.row(*nav)
    return kb.as_markup()


async def render_scams(page, admin):
    total = (await db_fetchone("SELECT COUNT(*) FROM scams"))[0]
    if not total:
        return "Скам-база пуста.", None
    page, pages = page_of(page, total, PAGE_SIZE)
    rows = await db_fetch(
        "SELECT tg_user_id, username, reason FROM scams"
        " ORDER BY added_at DESC, tg_user_id DESC LIMIT ? OFFSET ?",
        PAGE_SIZE, page * PAGE_SIZE,
    )
    lines = []
    for i, (uid, uname, reason) in enumerate(rows, page * PAGE_SIZE + 1):
        who = (
            f"@{html.escape(uname)}"
            if uname
            else f'<a href="tg://user?id={uid}">user {uid}</a>'
        )
        line = f"{i}. {who} · id {uid}"
        if reason:
            short = reason if len(reason) <= 100 else reason[:100] + "…"
            line += f" · {html.escape(short)}"
        lines.append(line)
    text = (
        f"🚫 <b>Скам-база ({total})</b>\nСтраница {page + 1} из {pages}\n\n"
        + "\n".join(lines)
    )
    return text, kb_scams(page, pages, rows if admin else None)


# --- handlers ---
async def start(msg):
    text, kb = await render_menu(0, msg.from_user.id)
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
        kb.button(
            text=f"❌ {(title or 'чат')[:30]}", callback_data=f"rm:{chat_id}", style="danger"
        )
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
    kb.button(text="📥 Получить БД", callback_data="adm:get", style="primary")
    kb.button(text="📤 Загрузить БД", callback_data="adm:put", style="success")
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


async def scam_list(msg, command: CommandObject):
    arg = (command.args or "").strip()
    page = int(arg) - 1 if arg.isdigit() else 0
    text, kb = await render_scams(page, msg.from_user.id == ADMIN_ID)
    await msg.answer(text, reply_markup=kb)


async def cb_scam_page(cb):
    await cb.answer()
    page = int(cb.data.split(":")[1])
    text, kb = await render_scams(page, cb.from_user.id == ADMIN_ID)
    await safe_edit(cb.message, text, kb)


async def cb_scam_del(cb):
    if cb.from_user.id != ADMIN_ID:
        await cb.answer("Нет доступа")
        return
    try:
        _, _, uid, page = cb.data.split(":")
        await db_exec("DELETE FROM scams WHERE tg_user_id=?", int(uid))
        text, kb = await render_scams(int(page), True)
    except Exception:
        logger.exception("cb_scam_del упал")
        await cb.answer(ERR)
        return
    await cb.answer("Удалил")
    await safe_edit(cb.message, text, kb)


async def help_msg(msg):
    await msg.answer(HELP)


async def home_cb(cb):
    await cb.answer()
    text, kb = await render_menu(0, cb.from_user.id)
    await safe_edit(cb.message, text, kb)


async def menu_cb(cb):
    await cb.answer()
    page = int(cb.data.split(":")[1])
    text, kb = await render_menu(page, cb.from_user.id)
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
        text, kb = await render_menu(0, cb.from_user.id)
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
    dp.message.register(scam_list, Command("sl", "scamlist"))
    dp.callback_query.register(cb_scam_del, F.data.startswith("sc:del:"))
    dp.callback_query.register(cb_scam_page, F.data.regexp(r"^sc:\d+$"))
    dp.callback_query.register(home_cb, F.data == "home")
    dp.callback_query.register(menu_cb, F.data.startswith("menu:"))
    dp.callback_query.register(model_cb, F.data.startswith("m:"))
    dp.callback_query.register(filt_cb, F.data.startswith("f:"))
    dp.callback_query.register(add_hint_cb, F.data == "add_hint")
    dp.callback_query.register(remove_cb, F.data.startswith("rm:"))
    dp.callback_query.register(admin_get_cb, F.data == "adm:get")
    dp.business_connection.register(on_business_connection)
    dp.business_message.register(on_business_message, F.text)
    dp.callback_query.register(admin_put_cb, F.data == "adm:put")


async def set_commands():
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Выбрать модель"),
            BotCommand(command="add", description="Добавить чат"),
            BotCommand(command="info", description="Статистика"),
            BotCommand(command="remove", description="Убрать чат"),
            BotCommand(command="sl", description="Скам-база"),
            BotCommand(command="help", description="Как пользоваться"),
        ]
    )
    await bot.set_chat_menu_button(menu_button=MenuButtonCommands())


# --- business (скрытая фича, в /help не упоминается) ---
BIZ_PAGE = 30


async def refresh_models(user_id):
    rows = await db_fetch(
        f"SELECT model_slug, MIN(name) FROM nfts WHERE {NOT_SCAM}"
        " GROUP BY model_slug ORDER BY MIN(name)"
    )
    await db_exec("DELETE FROM biz_models WHERE user_id=?", user_id)
    await db_many(
        "INSERT INTO biz_models VALUES (?,?,?,?)",
        [(user_id, i + 1, slug, name) for i, (slug, name) in enumerate(rows)],
    )
    return [(i + 1, name) for i, (_, name) in enumerate(rows)]


def models_chunks(rows):
    if not rows:
        return ["Пока пусто: в базе ещё нет NFT-моделей."]
    example = html.escape(rows[0][1])
    guide = (
        "Как пользоваться: напиши в любом чате /parse 1 — это модель номер 1, "
        f"например {example}. Бот заменит твоё сообщение списком из {BIZ_PAGE} "
        "пользователей. Напишешь ещё раз — получишь следующую страницу. "
        "Обновить список моделей: /models"
    )
    chunks, cur = [], "Список NFT подарков:\n\n"
    for n, name in rows:
        line = f"{n}. {html.escape(name)}\n"
        if len(cur) + len(line) > 3500:
            chunks.append(cur)
            cur = ""
        cur += line
    chunks.append(cur + "\n" + guide)
    return chunks


async def biz_owner_id(cid):
    if cid in biz_owner:
        return biz_owner[cid]
    row = await db_fetchone("SELECT user_id FROM biz_conn WHERE connection_id=?", cid)
    if row:
        biz_owner[cid] = row[0]
        return row[0]
    try:
        bc = await bot.get_business_connection(business_connection_id=cid)
    except Exception as e:
        logger.warning("business connection %s не получил: %s", cid, e)
        return None
    await db_exec(
        "INSERT OR REPLACE INTO biz_conn VALUES (?,?,?,?)",
        cid, bc.user.id, bc.user_chat_id, 1 if bc.is_enabled else 0,
    )
    biz_owner[cid] = bc.user.id
    return bc.user.id


async def on_business_connection(bc):
    try:
        can_reply = getattr(bc, "can_reply", None)
        rights = getattr(bc, "rights", None)
        if can_reply is None and rights is not None:
            can_reply = getattr(rights, "can_reply", None)
        await db_exec(
            "INSERT OR REPLACE INTO biz_conn VALUES (?,?,?,?)",
            bc.id, bc.user.id, bc.user_chat_id, 1 if bc.is_enabled else 0,
        )
        biz_owner[bc.id] = bc.user.id
        if not bc.is_enabled:
            return
        text = "✅ Бот подключен"
        if can_reply is False:
            text += "\n\n⚠️ Включи право отвечать на сообщения в настройках чат-бота."
        await bot.send_message(bc.user_chat_id, text)
        for chunk in models_chunks(await refresh_models(bc.user.id)):
            await bot.send_message(bc.user_chat_id, chunk)
    except Exception:
        logger.exception("business_connection упал")


async def biz_reply(msg, text):
    cid = msg.business_connection_id
    try:
        await bot.edit_message_text(
            text,
            business_connection_id=cid,
            chat_id=msg.chat.id,
            message_id=msg.message_id,
        )
    except TelegramBadRequest as e:
        logger.warning("не отредактировал сообщение, шлю новое: %s", e)
        await bot.send_message(msg.chat.id, text, business_connection_id=cid)


async def biz_parse(owner, n):
    q_sql = "SELECT slug, name FROM biz_models WHERE user_id=? AND num=?"
    row = await db_fetchone(q_sql, owner, n)
    if not row:
        await refresh_models(owner)
        row = await db_fetchone(q_sql, owner, n)
    if not row:
        return f"Модели с номером {n} нет. Список: /models"
    slug, name = row
    total = (
        await db_fetchone(
            "SELECT COUNT(DISTINCT owner_user_id) FROM nfts"
            f" WHERE model_slug=? AND {NOT_SCAM}", slug
        )
    )[0]
    if not total:
        return f"По модели «{html.escape(name)}» пока пусто."
    pages = -(-total // BIZ_PAGE)
    st = await db_fetchone(
        "SELECT page FROM biz_pages WHERE user_id=? AND model_slug=?", owner, slug
    )
    page = st[0] if st else 0
    if page >= pages:
        page = 0
    await db_exec(
        "INSERT OR REPLACE INTO biz_pages VALUES (?,?,?)",
        owner, slug, page + 1 if page + 1 < pages else 0,
    )
    rows = await db_fetch(
        "SELECT name, gift_num, owner_user_id, owner_username, nft_link, profile_link"
        f" FROM nfts WHERE model_slug=? AND {NOT_SCAM}"
        " GROUP BY owner_user_id HAVING id = MAX(id)"
        " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
        slug, BIZ_PAGE, page * BIZ_PAGE,
    )
    lines = []
    for i, (nm, num_, uid, uname, nft_link, profile) in enumerate(rows):
        who = f"@{html.escape(uname)}" if uname else f"user {uid}"
        lines.append(
            f"{page * BIZ_PAGE + i + 1}. "
            f'<a href="{profile}">{who}</a> - '
            f'<a href="{nft_link}">{html.escape(nm)} (#{num_})</a>'
        )
    head = f"<b>{html.escape(name)}</b> — страница {page + 1} из {pages}"
    return head + "\n\n" + "\n".join(lines)


async def on_scam_cmd(msg, text):
    owner = await biz_owner_id(msg.business_connection_id)
    if owner != ADMIN_ID or msg.from_user.id != owner or msg.chat.id == owner:
        return
    parts = text.split(maxsplit=1)
    reason = parts[1].strip()[:200] if len(parts) > 1 else ""
    try:
        await add_scam(msg.chat.id, msg.chat.username, reason or None, owner)
        out = "ok, добавил в скам-базу"
        if reason:
            out += f": {html.escape(reason)}"
        await biz_reply(msg, out)
    except Exception:
        logger.exception("on_scam_cmd упал")


async def on_business_message(msg):
    text = (msg.text or "").strip()
    if not text.startswith("/") or not msg.from_user:
        return
    if re.match(r"^/scam(?:@\w+)?(?:\s|$)", text, re.I):
        await on_scam_cmd(msg, text)
        return
    m = re.match(r"^/(parse|models)(?:@\w+)?(?:\s+(\S+))?\s*$", text, re.I)
    if not m:
        return
    cid = msg.business_connection_id
    owner = await biz_owner_id(cid)
    if not owner or msg.from_user.id != owner:
        return  # команды работают только от владельца аккаунта
    try:
        if m.group(1).lower() == "models":
            chunks = models_chunks(await refresh_models(owner))
            await biz_reply(msg, chunks[0])
            for extra in chunks[1:]:
                await bot.send_message(msg.chat.id, extra, business_connection_id=cid)
            return
        arg = m.group(2)
        if arg and arg.isdigit():
            out = await biz_parse(owner, int(arg))
        else:
            out = "Напиши /parse и номер модели, например /parse 1. Список: /models"
        await biz_reply(msg, out)
    except Exception:
        logger.exception("business_message упал")


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
    global bot, dp
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
    for i, sess in enumerate(SESSIONS):
        c = TelegramClient(StringSession(sess), API_ID, API_HASH)
        try:
            await c.connect()
            if not await c.is_user_authorized():
                logger.error("аккаунт %s: сессия не рабочая, пропускаю", i + 1)
                await c.disconnect()
                continue
            a = Acc(i, c)
            a.me = (await c.get_me()).id
        except Exception:
            logger.exception("аккаунт %s не подключился", i + 1)
            continue
        try:
            await c.get_dialogs()
        except Exception as e:
            logger.warning("аккаунт %s: диалоги не загрузились: %s", i + 1, e)
        accs.append(a)
    if not accs:
        logger.error("нет рабочих аккаунтов в SESSION_STRING")
        return
    logger.info("аккаунтов в работе: %s", len(accs))
    running.set()
    await restore_pending()
    job = asyncio.create_task(worker())
    night = asyncio.create_task(nightly())
    ping = None
    try:
        if HOST:
            await bot.set_webhook(
                url=f"https://{HOST}{WEBHOOK_PATH}",
                secret_token=SECRET or None,
                allowed_updates=dp.resolve_used_update_types(),
            )
            logger.info("вебхук стоит: https://%s%s", HOST, WEBHOOK_PATH)
            ping = asyncio.create_task(keep_alive())
            await asyncio.Event().wait()
        else:
            await bot.delete_webhook()
            await dp.start_polling(bot)
    finally:
        job.cancel()
        night.cancel()
        if ping:
            ping.cancel()
        if runner:
            await runner.cleanup()
        for a in accs:
            await a.client.disconnect()
        await bot.session.close()
        if _db:
            await _db.close()


if __name__ == "__main__":
    asyncio.run(main())
