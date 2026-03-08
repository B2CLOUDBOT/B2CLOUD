import asyncio
import logging
import io
import zipfile
import aiohttp
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from motor.motor_asyncio import AsyncIOMotorClient
from aiogram.exceptions import TelegramBadRequest

# ============================================================
# CONFIGURATION
# ============================================================
IST = ZoneInfo('Asia/Kolkata')

def now_ist():
    return datetime.now(IST)

def now_db():
    return datetime.now()

API_TOKEN = os.environ["API_TOKEN"]
MONGO_URI = os.environ["MONGO_URI"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
STORAGE_CHANNEL = int(os.environ["STORAGE_CHANNEL"])

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()
client = AsyncIOMotorClient(MONGO_URI)
db = client.personal_cloud_db
albums_col = db.albums
b2_history_col = db.b2_history

user_sessions = {}
view_sessions = {}     # uid -> True  (jab /view chal raha ho toh)
password_pending = {}  # uid -> {"action": "view"/"zip", "album_id": ..., "album": ...}
granted_users: set = set()

# ── Registration code generator ──────────────────────────────
async def get_or_create_reg_code(uid: int) -> str:
    """Har user ka permanent unique registration code."""
    existing = await db.reg_codes.find_one({"user_id": uid})
    if existing:
        return existing["code"]
    count = await db.reg_codes.count_documents({})
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    digits  = "123456789"
    total   = len(letters) * len(digits)  # 234
    if count < total:
        l = letters[count // len(digits)]
        d = digits[count % len(digits)]
        code = f"{l}{d}"
    else:
        count2 = count - total
        l1 = letters[(count2 // (len(letters) * len(digits)))]
        l2 = letters[(count2 // len(digits)) % len(letters)]
        d  = digits[count2 % len(digits)]
        code = f"{l1}{l2}{d}"
    await db.reg_codes.insert_one({"user_id": uid, "code": code, "created_at": now_db()})
    return code


# ============================================================
# HELPERS
# ============================================================
def is_owner(uid): return uid == ADMIN_ID
def is_admin(uid): return uid == ADMIN_ID or uid in granted_users

async def find_album(identifier: str):
    identifier = identifier.strip()
    return await albums_col.find_one({
        "$or": [
            {"name": {"$regex": f"^{re.escape(identifier)}$", "$options": "i"}},
            {"album_id": identifier}
        ]
    })

def auto_generate_tags(name: str) -> list:
    name_lower = name.lower().strip()
    words = re.split(r'[\s_\-]+', name_lower)
    words = [w for w in words if w and len(w) >= 2]
    tags = set()
    for w in words:
        tags.add(f"#{w}")
    for i in range(len(words) - 1):
        tags.add(f"#{words[i]}{words[i+1]}")
    if len(words) >= 3:
        tags.add(f"#{''.join(words)}")
        for i in range(len(words) - 2):
            tags.add(f"#{words[i]}{words[i+1]}{words[i+2]}")
    return sorted(tags)


def count_media(files):
    photos = videos = docs = audios = 0
    for item in files:
        t = item.get("type", "photo") if isinstance(item, dict) else "photo"
        if t == "video": videos += 1
        elif t == "document": docs += 1
        elif t in ("audio", "voice"): audios += 1
        else: photos += 1
    return photos, videos, docs, audios

async def send_to_storage(fid: str, mtype: str):
    for attempt in range(5):
        try:
            if mtype == "video":
                msg = await bot.send_video(STORAGE_CHANNEL, fid)
                fsize = msg.video.file_size if msg.video else 0
            elif mtype == "document":
                msg = await bot.send_document(STORAGE_CHANNEL, fid)
                fsize = msg.document.file_size if msg.document else 0
            elif mtype == "audio":
                msg = await bot.send_audio(STORAGE_CHANNEL, fid)
                fsize = msg.audio.file_size if msg.audio else 0
            elif mtype == "voice":
                msg = await bot.send_voice(STORAGE_CHANNEL, fid)
                fsize = msg.voice.file_size if msg.voice else 0
            else:
                msg = await bot.send_photo(STORAGE_CHANNEL, fid)
                fsize = msg.photo[-1].file_size if msg.photo else 0
            return msg.message_id, fsize
        except Exception as e:
            err_str = str(e)
            if "Too Many Requests" in err_str or "Flood" in err_str:
                wait_match = re.search(r"retry after (\d+)", err_str)
                wait_sec = int(wait_match.group(1)) if wait_match else 30
                wait_sec += 2
                logger.warning(f"Flood control! Waiting {wait_sec}s (attempt {attempt+1}/5)")
                await asyncio.sleep(wait_sec)
                continue
            else:
                logger.error(f"Storage send error: {e}")
                return None, 0
    logger.error(f"Storage send failed after 5 retries: {fid}")
    return None, 0


# ============================================================
# CHECKLIST HELPERS
# ============================================================
def get_channel_id_for_link(channel_id: int) -> str:
    """STORAGE_CHANNEL is negative like -1003454871680 → strip -100 → 3454871680"""
    s = str(channel_id)
    if s.startswith("-100"):
        return s[4:]
    return s.lstrip("-")

def ordinal(n: int) -> str:
    """1 → 01st, 2 → 02nd, 3 → 03rd, 4 → 04th ..."""
    n = int(n)
    suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    if 11 <= n % 100 <= 13: suffix = "th"
    return f"{n:02d}{suffix}"

async def rebuild_checklist_text() -> str:
    """Saare albums fetch karo aur checklist text banao with add history links."""
    setting = await db.settings.find_one({"key": "checklist_title"})
    title = setting["value"] if setting else "B2 CLOUD"
    albums = await albums_col.find().sort("created_at", 1).to_list(200)
    ch_id = get_channel_id_for_link(STORAGE_CHANNEL)
    lines = []
    for alb in albums:
        name       = alb.get("name", "Unnamed")
        msg_id     = alb.get("created_msg_id")
        add_history = alb.get("add_history", [])  # list of {msg_id, count, at}
        # Album name line
        if msg_id:
            link = f"https://t.me/c/{ch_id}/{msg_id}"
            lines.append(f"┃ ⚜ [{name}]({link})")
        else:
            lines.append(f"┃ ⚜ {name}")
        # Add history lines — 01st Added, 02nd Added ...
        for idx, entry in enumerate(add_history, 1):
            add_mid = entry.get("msg_id")
            if add_mid:
                add_link = f"https://t.me/c/{ch_id}/{add_mid}"
                lines.append(f"┃       [{ordinal(idx)} Added]({add_link})")
            else:
                lines.append(f"┃       {ordinal(idx)} Added")
    body = "\n┃\n".join(lines) if lines else "┃ _(koi album nahi)_"
    text = (
        f"┏━━━━━━━✦❘༻༺❘✦━━━━━━━┓\n"
        f"┃     👑 {title} 👑\n"
        f"┃▰▱▱▱▱▱▱▱▱▱▱▱▱▱▱▰\n"
        f"┃\n"
        f"{body}\n"
        f"┃\n"
        f"┃▰▱▱▱▱▱▱▱▱▱▱▱▱▱▱▰\n"
        f"┃\n"
        f"┗━━━━━━━✦❘༻༺❘✦━━━━━━━┛"
    )
    return text

async def update_checklist():
    """Pinned checklist message ko update karo."""
    try:
        setting = await db.settings.find_one({"key": "checklist_msg_id"})
        if not setting:
            return  # checklist exist nahi karta — skip silently
        msg_id = setting["value"]
        new_text = await rebuild_checklist_text()
        await bot.edit_message_text(
            chat_id=STORAGE_CHANNEL,
            message_id=msg_id,
            text=new_text,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.warning(f"Checklist update failed: {e}")



# ============================================================
# /start
# ============================================================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    uid = message.from_user.id
    username = (message.from_user.username or "").lower()
    first_name = message.from_user.first_name or "there"

    if username:
        pending = await db.granted_users.find_one({"username": username, "pending": True})
        if pending:
            granted_users.add(uid)
            await db.granted_users.update_one(
                {"username": username},
                {"$set": {"user_id": uid, "username": username, "full_name": message.from_user.full_name, "pending": False}}
            )
            logger.info(f"✅ Pending grant activated: @{username} = {uid}")

    # ── Unknown user ─────────────────────────────────────────
    if not is_admin(uid):
        reg_code = await get_or_create_reg_code(uid)
        is_denied = await db.denied_users.find_one({"user_id": uid}) is not None
        prev = await db.granted_users.find_one({"user_id": uid})
        is_old = is_denied or (prev is not None)
        emoji_status = "🔴 old" if is_old else "🆕 new"
        user = message.from_user
        uname = f"@{user.username}" if user.username else "N/A"
        grant_str = f"@{user.username}" if user.username else str(uid)
        await bot.send_message(
            ADMIN_ID,
            f"👤 {user.full_name} /start\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🎫 Code: *{reg_code}*\n"
            f"🆔 User ID: `{uid}`\n"
            f"📛 Name: {user.full_name}\n"
            f"🔗 Username: {uname}\n"
            f"📊 Status: {emoji_status}\n"
            f"✅ Access: `/grant {grant_str}`",
            parse_mode="Markdown"
        )
        await message.answer(
            f"☁️ *Personal Cloud Bot*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Yeh ek private cloud storage bot hai.\n"
            f"Abhi aapke paas is bot ka access nahi hai.\n\n"
            f"🆔 /id — Apna User ID dekho",
            parse_mode="Markdown"
        )
        return

    # ── Common commands (owner + granted both) ────────────────
    common = (
        "☁️ *Personal Cloud Bot*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📁 *Album Management*\n"
        "┣ /album `<name>` — Naya album banao\n"
        "┣ /add `<name/id>` — Files add karo\n"
        "┣ /close — Save karo ya view rokna\n"
        "┗ /cancel — Session cancel karo\n\n"
        "🗂 *Organize*\n"
        "┣ /lock `<name/id>` — Album lock karo\n"
        "┣ /unlock `<name/id>` — Album unlock karo\n"
        "┣ /rename `<old>` `<new>` — Album rename karo\n"
        "┣ /merge `<id1>` `<id2>` `<name>` — Merge karo\n"
        "┣ /tag `<name/id>` `#tag1` `#tag2` — Tags lagao\n"
        "┣ /dlt `<name/id>` — Files selectively hatao\n"
        "┣ /setpass `<name/id>` `<pass>` — Password lagao\n"
        "┗ /removepass `<name/id>` — Password hatao\n\n"
        "🔍 *View & Search*\n"
        "┣ /albums — Saare albums dekho\n"
        "┣ /view `<name/id>` — Album files dekho\n"
        "┣ /view `#tag1` `#tag2` — Tag se search karo\n"
        "┣ /info `<name/id>` — Album details\n"
        "┗ /stats — Cloud stats\n\n"
        "📤 *Share & Export*\n"
        "┣ /b2 `<id>` `@u1` `@u2` — Album share karo\n"
        "┗ /zip `<name/id>` — ZIP ya forward karo\n\n"
        "🆔 /id — Apna User ID dekho"
    )

    # ── Owner gets extra access section ──────────────────────
    if is_owner(uid):
        owner_extra = (
            "\n\n👑 *Owner Controls*\n"
            "┣ /grant `<id/@user>` — Access do\n"
            "┣ /denied `<id/@user>` — Access hatao\n"
            "┣ /list — Albums, users & share history\n"
            "┣ /idinfo — Granted users + albums\n"
            "┣ /idinfo `<id/@user>` — Kisi ka bhi info\n"
            "┣ /makelist `<title>` — Checklist banao\n"
            "┗ /removelist — Checklist hatao"
        )
        await message.answer(common + owner_extra, parse_mode="Markdown")
    else:
        await message.answer(common, parse_mode="Markdown")


# ============================================================
# ALBUM CREATION - /album
# ============================================================
@dp.message(Command("album"))
async def cmd_album(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/album TripName`", parse_mode="Markdown")

    name = args[1].strip()
    existing = await albums_col.find_one({"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}})
    if existing:
        return await message.answer(
            f"⚠️ Album **'{name}'** already exists!\n"
            f"ID: `{existing['album_id']}` | Files: {existing['count']}\n"
            f"Use `/add {name}` to add more files.",
            parse_mode="Markdown"
        )

    if message.from_user.id in user_sessions:
        active = user_sessions[message.from_user.id]
        files_count = len(active.get("photos", []))
        builder = InlineKeyboardBuilder()
        builder.row(
            types.InlineKeyboardButton(text="❌ Pehla Cancel Karo", callback_data="warn_cancel_first"),
            types.InlineKeyboardButton(text="✅ Pehla Save Karo", callback_data="warn_save_first"),
        )
        return await message.answer(
            f"⚠️ **Active Session Already Hai!**\n\n"
            f"📁 Album: **{active.get('name', '?')}**\n"
            f"🗂 Files: {files_count} abhi tak\n\n"
            f"Pehle is session ko `/close` ya `/cancel` karo,\ntabhi naya album bana sakte ho!",
            reply_markup=builder.as_markup(),
            parse_mode="Markdown"
        )

    user_sessions[message.from_user.id] = {
        "mode": "create", "name": name,
        "photos": [], "ids": set(), "started_at": now_db()
    }

    await message.answer(
        f"📸 **Album Creation Started!**\n\n"
        f"📁 Name: **{name}**\n"
        f"📤 Files bhejiye (photo/video/pdf/audio)\n"
        f"✅ Done? `/close` likhein\n"
        f"❌ Cancel? `/cancel` likhein",
        parse_mode="Markdown"
    )


# ============================================================
# MEDIA HANDLER
# ============================================================
async def _handle_media(message: types.Message, file_id: str, unique_id: str, media_type: str, fname: str = "", file_size: int = 0):
    uid = message.from_user.id
    if uid not in user_sessions:
        return
    session = user_sessions[uid]
    if unique_id in session["ids"]:
        return await message.reply(f"🚫 Duplicate {media_type}! Skip kar diya.")
    session["photos"].append({"file_id": file_id, "type": media_type, "name": fname, "file_size": file_size})
    session["ids"].add(unique_id)

@dp.message(F.photo)
async def handle_photo(message: types.Message):
    if message.from_user.id not in user_sessions: return
    p = message.photo[-1]
    await _handle_media(message, p.file_id, p.file_unique_id, "photo", file_size=p.file_size or 0)

@dp.message(F.video)
async def handle_video(message: types.Message):
    if message.from_user.id not in user_sessions: return
    await _handle_media(message, message.video.file_id, message.video.file_unique_id, "video", file_size=message.video.file_size or 0)

@dp.message(F.document)
async def handle_document(message: types.Message):
    if message.from_user.id not in user_sessions: return
    d = message.document
    await _handle_media(message, d.file_id, d.file_unique_id, "document", d.file_name or "", file_size=d.file_size or 0)

@dp.message(F.audio)
async def handle_audio(message: types.Message):
    if message.from_user.id not in user_sessions: return
    await _handle_media(message, message.audio.file_id, message.audio.file_unique_id, "audio", file_size=message.audio.file_size or 0)

@dp.message(F.voice)
async def handle_voice(message: types.Message):
    if message.from_user.id not in user_sessions: return
    await _handle_media(message, message.voice.file_id, message.voice.file_unique_id, "voice", file_size=message.voice.file_size or 0)


# ============================================================
# Quick action callbacks
# ============================================================
@dp.callback_query(F.data == "quick_close")
async def quick_close(callback: types.CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid]["mode"] != "create":
        return await callback.message.answer("⚠️ Koi active album creation session nahi hai.")
    session = user_sessions[uid]
    if not session["photos"]:
        del user_sessions[uid]
        return await callback.message.answer("⚠️ Koi file nahi thi. Session cancel ho gaya.")
    auto_id = f"ALB-{now_ist().strftime('%y%m%d%H%M')}"
    duration = (now_ist() - session["started_at"]).seconds // 60
    photos, videos, docs, audios = count_media(session["photos"])
    stats = ""
    if photos: stats += f"📸 {photos} photos\n"
    if videos: stats += f"🎥 {videos} videos\n"
    if docs: stats += f"📄 {docs} documents\n"
    if audios: stats += f"🎵 {audios} audio\n"
    preview_caption = (
        f"📝 **ALBUM PREVIEW**\n━━━━━━━━━━━━━━━━━━\n"
        f"📁 Name: **{session['name']}**\n🆔 ID: `{auto_id}`\n{stats}"
        f"⏱ Session: ~{duration} min\n━━━━━━━━━━━━━━━━━━\nSave karna chahte hain?"
    )
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="✅ Save Album", callback_data="confirm_save"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="confirm_cancel")
    )
    first = session["photos"][0]
    fid = first["file_id"] if isinstance(first, dict) else first
    mtype = first.get("type", "photo") if isinstance(first, dict) else "photo"
    try:
        if mtype == "video":
            await bot.send_video(callback.message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        elif mtype == "document":
            await bot.send_document(callback.message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        else:
            await bot.send_photo(callback.message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
    except TelegramBadRequest as e:
        logger.error(f"Preview error: {e}")
        await callback.message.answer("❌ Preview generate nahi ho saka.")

@dp.callback_query(F.data == "quick_save_add")
async def quick_save_add_cb(callback: types.CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid]["mode"] != "add":
        return await callback.message.answer("⚠️ Koi active add session nahi hai.")
    session = user_sessions[uid]
    if not session["photos"]:
        del user_sessions[uid]
        return await callback.message.answer("⚠️ Koi file nahi bheji.")
    new_count = len(session["photos"])
    new_photos, new_videos, new_docs, new_audios = count_media(session["photos"])
    saved_items = []
    for item in session["photos"]:
        fid = item["file_id"] if isinstance(item, dict) else item
        mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
        mid, fsize = await send_to_storage(fid, mtype)
        new_item = dict(item) if isinstance(item, dict) else {"file_id": fid, "type": mtype, "name": ""}
        if mid: new_item["storage_msg_id"] = mid
        if fsize: new_item["file_size"] = fsize
        saved_items.append(new_item)
        await asyncio.sleep(0.2)
    await albums_col.update_one(
        {"_id": session["db_id"]},
        {
            "$push": {"photos": {"$each": saved_items}, "history": {"action": "added", "count": new_count, "by": uid, "at": now_db()}},
            "$inc": {"count": new_count, "media_count.photos": new_photos, "media_count.videos": new_videos, "media_count.docs": new_docs, "media_count.audios": new_audios},
            "$set": {"updated_at": now_db()}
        }
    )
    add_msg_id3 = None
    try:
        add_msg3 = await bot.send_message(STORAGE_CHANNEL,
            f"➕ **Files Added**\n📁 {session['name']} | 🆔 `{session['album_id']}`\n"
            f"🗂 +{new_count} files\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
            parse_mode="Markdown")
        add_msg_id3 = add_msg3.message_id
    except: pass
    await albums_col.update_one(
        {"_id": session["db_id"]},
        {"$push": {"add_history": {"msg_id": add_msg_id3, "count": new_count, "at": now_db()}}}
    )
    await update_checklist()
    await callback.message.answer(f"✅ **+{new_count} files** add ho gayi!\n📁 **{session['name']}**", parse_mode="Markdown")
    del user_sessions[uid]

@dp.callback_query(F.data == "quick_cancel")
async def quick_cancel_cb(callback: types.CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid in user_sessions:
        del user_sessions[uid]
    await callback.message.answer("❌ Session cancel ho gaya.")


@dp.callback_query(F.data == "warn_cancel_first")
async def warn_cancel_first(callback: types.CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid in user_sessions:
        session = user_sessions[uid]
        files_count = len(session.get("photos", []))
        del user_sessions[uid]
        await callback.message.edit_text(
            f"❌ **Pehla session cancel ho gaya!**\n"
            f"📁 {session.get('name', '?')} | 🗂 {files_count} files discard\n\n"
            f"Ab /album se naya album banao.",
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text("⚠️ Koi active session nahi tha.", parse_mode="Markdown")

@dp.callback_query(F.data == "warn_save_first")
async def warn_save_first(callback: types.CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid not in user_sessions:
        return await callback.message.edit_text("⚠️ Session expire ho gaya.", parse_mode="Markdown")
    await callback.message.edit_text(
        "✅ Theek hai! Pehle /close karke save karo,\nphir naya album banao.",
        parse_mode="Markdown"
    )

# ============================================================
# /close - Preview & Save (create mode) OR Save add session OR Stop view
# ============================================================
@dp.message(Command("close"))
async def cmd_close(message: types.Message):
    uid = message.from_user.id

    # ── Case 1: "add" session ────────────────────────────────
    if uid in user_sessions and user_sessions[uid]["mode"] == "add":
        session = user_sessions[uid]
        if not session["photos"]:
            del user_sessions[uid]
            return await message.answer("⚠️ Koi file nahi bheji. Session cancel.")
        # Save karo silently (same logic as save_add)
        try:
            new_count = len(session["photos"])
            new_photos, new_videos, new_docs, new_audios = count_media(session["photos"])
            user = message.from_user
            user_info = f"@{user.username}" if user.username else f"ID: {user.id}"
            save_msg = await message.answer(
                f"⏳ **Files save ho rahi hain...**\n📁 {session['name']}",
                parse_mode="Markdown"
            )
            try:
                await bot.send_message(STORAGE_CHANNEL, f"📁 **Files Added**\nName: {session['name']}\nBy: {user_info}", parse_mode="Markdown")
            except: pass
            saved_items = []
            total_new = len(session["photos"])
            for idx, item in enumerate(session["photos"], 1):
                fid = item["file_id"] if isinstance(item, dict) else item
                mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
                mid, fsize = await send_to_storage(fid, mtype)
                new_item = dict(item) if isinstance(item, dict) else {"file_id": fid, "type": mtype, "name": ""}
                if mid: new_item["storage_msg_id"] = mid
                if fsize: new_item["file_size"] = fsize
                saved_items.append(new_item)
                await asyncio.sleep(0.2)
                if idx % 5 == 0 or idx == total_new:
                    try:
                        await save_msg.edit_text(
                            f"⏳ Uploading... {idx}/{total_new}\n📁 {session['name']}",
                            parse_mode="Markdown"
                        )
                    except: pass
            await albums_col.update_one(
                {"_id": session["db_id"]},
                {
                    "$push": {"photos": {"$each": saved_items}, "history": {"action": "added", "count": new_count, "by": uid, "at": now_db()}},
                    "$inc": {"count": new_count, "media_count.photos": new_photos, "media_count.videos": new_videos, "media_count.docs": new_docs, "media_count.audios": new_audios},
                    "$set": {"updated_at": now_db()}
                }
            )
            add_msg_id = None
            try:
                add_msg = await bot.send_message(STORAGE_CHANNEL,
                    f"➕ **Files Added**\n📁 {session['name']} | 🆔 `{session['album_id']}`\n"
                    f"🗂 +{new_count} files\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
                    parse_mode="Markdown")
                add_msg_id = add_msg.message_id
            except: pass
            # Save add_history entry with msg_id
            await albums_col.update_one(
                {"_id": session["db_id"]},
                {"$push": {"add_history": {"msg_id": add_msg_id, "count": new_count, "at": now_db()}}}
            )
            await update_checklist()
            # Delete "uploading" msg aur send success
            try: await save_msg.delete()
            except: pass
            await message.answer(
                f"✅ **Successfully Saved!**\n\n"
                f"📁 Album: **{session['name']}**\n"
                f"🆔 `{session['album_id']}`\n"
                f"🗂 +{new_count} files added",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"close add-save error: {e}")
            await message.answer("❌ Save error. Retry karein.")
        del user_sessions[uid]
        return

    # ── Case 2: View session chal raha hai — stop karo ──────
    if uid in view_sessions:
        view_sessions[uid] = False
        return await message.answer("⏹ View band kar diya!")

    # ── Case 3: "create" session ─────────────────────────────
    if uid not in user_sessions or user_sessions[uid]["mode"] != "create":
        return await message.answer("⚠️ Koi active session nahi hai.")
    logger.info(f"cmd_close called by {uid}, session photos: {len(user_sessions[uid].get('photos', []))}")
    session = user_sessions[uid]
    if not session["photos"]:
        del user_sessions[uid]
        return await message.answer("⚠️ Koi file nahi thi. Session cancel ho gaya.")
    auto_id = f"ALB-{now_ist().strftime('%y%m%d%H%M')}"
    duration = (now_db() - session["started_at"]).seconds // 60
    photos, videos, docs, audios = count_media(session["photos"])
    stats = ""
    if photos: stats += f"📸 {photos} photos\n"
    if videos: stats += f"🎥 {videos} videos\n"
    if docs: stats += f"📄 {docs} documents\n"
    if audios: stats += f"🎵 {audios} audio\n"
    preview_caption = (
        f"📝 **ALBUM PREVIEW**\n━━━━━━━━━━━━━━━━━━\n"
        f"📁 Name: **{session['name']}**\n🆔 ID: `{auto_id}`\n{stats}"
        f"━━━━━━━━━━━━━━━━━━\nSave karna chahte hain?"
    )
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="✅ Save Album", callback_data="confirm_save"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="confirm_cancel")
    )
    first = session["photos"][0]
    fid = first["file_id"] if isinstance(first, dict) else first
    mtype = first.get("type", "photo") if isinstance(first, dict) else "photo"
    try:
        if mtype == "video":
            await bot.send_video(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        elif mtype == "document":
            await bot.send_document(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        else:
            await bot.send_photo(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Preview send error: {e}")
        try:
            await message.answer(preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        except Exception as e2:
            logger.error(f"Text fallback error: {e2}")
            await message.answer(f"❌ Preview error: {e}", parse_mode="Markdown")


# ============================================================
# CONFIRM SAVE / CANCEL
# ============================================================
@dp.callback_query(F.data.in_({"confirm_save", "confirm_cancel"}))
async def process_confirm(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_sessions:
        await callback.answer("Session expire ho gaya!", show_alert=True)
        try: await callback.message.delete()
        except: pass
        return

    session = user_sessions[uid]

    if callback.data == "confirm_save":
        album_id = f"ALB-{now_ist().strftime('%y%m%d%H%M%S')}"
        photos, videos, docs, audios = count_media(session["photos"])

        await callback.answer("⏳ Saving...")
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except: pass

        save_msg = await callback.message.answer(
            f"⏳ **Saving album...**\n📁 {session['name']}\n_Files storage pe upload ho rahi hain..._",
            parse_mode="Markdown"
        )

        user = callback.from_user
        user_info = f"@{user.username}" if user.username else f"ID: {user.id}"

        created_msg_id = None
        try:
            created_msg = await bot.send_message(
                STORAGE_CHANNEL,
                f"📁 **Album Created**\nName: {session['name']}\nCreated by: {user_info}",
                parse_mode="Markdown"
            )
            created_msg_id = created_msg.message_id
        except: pass

        # ── Step 1: Upload all files to storage channel ──────────
        saved_items = []
        total_files = len(session["photos"])
        for idx, item in enumerate(session["photos"], 1):
            fid = item["file_id"] if isinstance(item, dict) else item
            mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
            mid, fsize = await send_to_storage(fid, mtype)
            new_item = dict(item) if isinstance(item, dict) else {"file_id": fid, "type": mtype, "name": ""}
            if mid: new_item["storage_msg_id"] = mid
            if fsize: new_item["file_size"] = fsize
            saved_items.append(new_item)
            await asyncio.sleep(0.2)
            # Progress update har 5 files pe
            if idx % 5 == 0 or idx == total_files:
                try:
                    await save_msg.edit_text(
                        f"⏳ Uploading... {idx}/{total_files}\n📁 {session['name']}",
                        parse_mode="Markdown"
                    )
                except: pass

        # ── Step 2: Recalculate media counts from saved_items ────
        photos, videos, docs, audios = count_media(saved_items)

        album_doc = {
            "album_id": album_id,
            "name": session["name"],
            "photos": saved_items,
            "count": len(saved_items),
            "locked": False,
            "tags": auto_generate_tags(session["name"]),
            "created_by": uid,
            "created_by_username": callback.from_user.username or "",
            "created_at": now_db(),
            "updated_at": now_db(),
            "history": [{"action": "created", "count": len(saved_items), "by": uid, "at": now_db()}],
            "media_count": {"photos": photos, "videos": videos, "docs": docs, "audios": audios},
            "created_msg_id": created_msg_id
        }

        # ── Step 3: Save to MongoDB ───────────────────────────────
        db_saved = False
        try:
            await albums_col.insert_one(album_doc)
            db_saved = True
        except Exception as e:
            logger.error(f"MongoDB insert error: {e}")
            # Check if somehow it got saved
            existing = await albums_col.find_one({"album_id": album_id})
            if existing:
                db_saved = True

        # ── Step 4: Send "Album Saved & Stored" to storage channel ─
        stats_text = ""
        if photos: stats_text += f"📸 {photos} "
        if videos: stats_text += f"🎥 {videos} "
        if docs:   stats_text += f"📄 {docs} "
        if audios: stats_text += f"🎵 {audios} "

        if db_saved:
            # Storage channel confirmation — ALWAYS send this separately
            try:
                await bot.send_message(
                    STORAGE_CHANNEL,
                    f"✅ **Album Saved & Stored**\n"
                    f"🆔 ID: `{album_id}`\n"
                    f"📁 Name: {session['name']}\n"
                    f"🗂 Files: {len(saved_items)} ({stats_text.strip()})\n"
                    f"🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Storage channel message error: {e}")

            # Auto-update checklist
            await update_checklist()

            # "Uploading..." wala msg delete karo
            try: await save_msg.delete()
            except: pass
            # Preview (album thumbnail) wala msg delete karo
            try: await callback.message.delete()
            except: pass

            # Clean success message
            await callback.message.answer(
                f"✅ **Successfully Saved!**\n\n"
                f"📁 Album: **{session['name']}**\n"
                f"🆔 `{album_id}`\n"
                f"🗂 {len(saved_items)} documents",
                parse_mode="Markdown"
            )
        else:
            try:
                await save_msg.edit_text(
                    f"❌ **Save error!**\n📁 {session['name']}\nRetry: `/album {session['name']}`",
                    parse_mode="Markdown"
                )
            except: pass
    else:
        await callback.answer("❌ Cancelled")
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except: pass
        await callback.message.answer("❌ Album save cancel.")

    del user_sessions[uid]


# ============================================================
# /add - Add to existing album
# ============================================================
@dp.message(Command("add"))
async def cmd_add(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/add AlbumName` ya `/add ALB-xxx`", parse_mode="Markdown")
    name = args[1].strip()
    album = await find_album(name)
    if not album:
        return await message.answer(f"❌ **'{name}'** nahi mila.", parse_mode="Markdown")
    if album.get("locked"):
        return await message.answer(f"🔒 **'{album['name']}'** locked hai! Pehle `/unlock` karein.", parse_mode="Markdown")
    if message.from_user.id in user_sessions:
        del user_sessions[message.from_user.id]
    user_sessions[message.from_user.id] = {
        "mode": "add", "db_id": album["_id"],
        "album_id": album["album_id"], "name": album["name"],
        "photos": [], "ids": set(album.get("photo_unique_ids", [])),
        "started_at": now_db()
    }
    await message.answer(
        f"➕ **Adding to: {album['name']}**\n🆔 `{album['album_id']}` | Current: {album['count']} files\n\n"
        f"Files bhejein, phir `/close`\n❌ Cancel: `/cancel`",
        parse_mode="Markdown"
    )


# ============================================================
# /save_add
# ============================================================
@dp.message(Command("save_add"))
async def save_add(message: types.Message):
    uid = message.from_user.id
    if uid not in user_sessions or user_sessions[uid]["mode"] != "add":
        return await message.answer("⚠️ Koi active add session nahi hai.")
    session = user_sessions[uid]
    if not session["photos"]:
        del user_sessions[uid]
        return await message.answer("⚠️ Koi file nahi bheji. Session cancel.")
    try:
        new_count = len(session["photos"])
        new_photos, new_videos, new_docs, new_audios = count_media(session["photos"])
        user = message.from_user
        user_info = f"@{user.username}" if user.username else f"ID: {user.id}"
        try:
            await bot.send_message(STORAGE_CHANNEL, f"📁 **Files Added**\nName: {session['name']}\nBy: {user_info}", parse_mode="Markdown")
        except: pass
        saved_items = []
        for item in session["photos"]:
            fid = item["file_id"] if isinstance(item, dict) else item
            mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
            mid, fsize = await send_to_storage(fid, mtype)
            new_item = dict(item) if isinstance(item, dict) else {"file_id": fid, "type": mtype, "name": ""}
            if mid: new_item["storage_msg_id"] = mid
            if fsize: new_item["file_size"] = fsize
            saved_items.append(new_item)
            await asyncio.sleep(0.2)
        await albums_col.update_one(
            {"_id": session["db_id"]},
            {
                "$push": {"photos": {"$each": saved_items}, "history": {"action": "added", "count": new_count, "by": uid, "at": now_db()}},
                "$inc": {"count": new_count, "media_count.photos": new_photos, "media_count.videos": new_videos, "media_count.docs": new_docs, "media_count.audios": new_audios},
                "$set": {"updated_at": now_db()}
            }
        )
        add_msg_id2 = None
        try:
            add_msg2 = await bot.send_message(STORAGE_CHANNEL,
                f"➕ **Files Added**\n📁 {session['name']} | 🆔 `{session['album_id']}`\n"
                f"🗂 +{new_count} files\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
                parse_mode="Markdown")
            add_msg_id2 = add_msg2.message_id
        except: pass
        await albums_col.update_one(
            {"_id": session["db_id"]},
            {"$push": {"add_history": {"msg_id": add_msg_id2, "count": new_count, "at": now_db()}}}
        )
        await update_checklist()
        await message.answer(f"✅ **+{new_count} files** add ho gayi!\n📁 **{session['name']}**", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"save_add error: {e}")
        await message.answer("❌ Save error. Retry karein.")
    del user_sessions[uid]


# ============================================================
# /lock & /unlock
# ============================================================
@dp.message(Command("lock"))
async def cmd_lock(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/lock AlbumName`", parse_mode="Markdown")
    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"locked": True, "updated_at": now_db()}})
    await message.answer(f"🔒 **'{album['name']}'** locked!", parse_mode="Markdown")

@dp.message(Command("unlock"))
async def cmd_unlock(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/unlock AlbumName`", parse_mode="Markdown")
    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"locked": False, "updated_at": now_db()}})
    await message.answer(f"🔓 **'{album['name']}'** unlocked!", parse_mode="Markdown")


# ============================================================
# /rename
# ============================================================
@dp.message(Command("rename"))
async def cmd_rename(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    parts = message.text.split(maxsplit=1)
    text = parts[1].strip() if len(parts) > 1 else ""
    quoted = re.findall(r"['\"](.*?)['\"]+", text)
    if len(quoted) >= 2:
        old_name, new_name = quoted[0].strip(), quoted[1].strip()
    else:
        simple = text.split()
        if len(simple) < 2:
            return await message.answer("❌ Usage:\n`/rename OldName NewName`\n`/rename 'Old Name' 'New Name'`\n`/rename ALB-xxx NewName`", parse_mode="Markdown")
        old_name, new_name = simple[0], simple[1]
    album = await find_album(old_name)
    if not album: return await message.answer(f"❌ **'{old_name}'** nahi mila.", parse_mode="Markdown")
    conflict = await albums_col.find_one({"name": {"$regex": f"^{re.escape(new_name)}$", "$options": "i"}})
    if conflict: return await message.answer(f"⚠️ **'{new_name}'** already exists!", parse_mode="Markdown")
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"name": new_name, "updated_at": now_db()}})
    await update_checklist()
    await message.answer(f"📝 **{album['name']}** → **{new_name}**", parse_mode="Markdown")


# ============================================================
# /delete
# ============================================================
@dp.message(Command("delete"))
async def cmd_delete(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/delete AlbumName`", parse_mode="Markdown")
    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="🗑️ Haan, Delete", callback_data=f"del_yes_{album['album_id']}"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="del_no")
    )
    await message.answer(
        f"⚠️ **Delete Confirmation**\n\n📁 **{album['name']}**\n🆔 `{album['album_id']}`\n🗂 {album['count']} files\n\nYeh action **undo nahi** ho sakta!",
        reply_markup=builder.as_markup(), parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("del_"))
async def process_delete(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return await callback.answer("🚫 Access Denied!", show_alert=True)
    if callback.data == "del_no":
        await callback.answer("❌ Cancel")
        return await callback.message.edit_text("❌ Delete cancel.", parse_mode="Markdown")
    album_id = callback.data.replace("del_yes_", "")
    result = await albums_col.delete_one({"album_id": album_id})
    if result.deleted_count:
        try:
            await bot.send_message(STORAGE_CHANNEL, f"🗑️ **Album Deleted**\nID: `{album_id}`\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST", parse_mode="Markdown")
        except: pass
        await update_checklist()
        await callback.message.edit_text(f"🗑️ Album deleted!\nID: `{album_id}`", parse_mode="Markdown")
    else:
        await callback.message.edit_text("❌ Delete nahi ho saka.", parse_mode="Markdown")
    await callback.answer()


# ============================================================
# /dlt - Selective file delete
# ============================================================
@dp.message(Command("dlt"))
async def cmd_dlt(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/dlt AlbumName` ya `/dlt ALB-xxx`", parse_mode="Markdown")
    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")
    if album.get("locked"): return await message.answer("🔒 Album locked hai!", parse_mode="Markdown")
    files = album.get("photos", [])
    if not files: return await message.answer("❌ Album empty hai.", parse_mode="Markdown")
    user_sessions[message.from_user.id] = {
        "mode": "dlt", "album_id": album["album_id"],
        "album_name": album["name"], "files": files, "selected": set()
    }
    await message.answer(
        f"🗑️ **Selective Delete: {album['name']}**\n🗂 {len(files)} files\n\nAb files bhej raha hoon — har ek ke niche ✅/❌ button hoga.",
        parse_mode="Markdown"
    )
    for idx, item in enumerate(files):
        fid = item["file_id"] if isinstance(item, dict) else item
        mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Keep", callback_data=f"dlt_toggle_{album['album_id']}_{idx}_keep")
        caption = f"File #{idx+1} | {mtype}"
        try:
            if mtype == "video": await bot.send_video(message.chat.id, fid, caption=caption, reply_markup=kb.as_markup())
            elif mtype == "document": await bot.send_document(message.chat.id, fid, caption=caption, reply_markup=kb.as_markup())
            else: await bot.send_photo(message.chat.id, fid, caption=caption, reply_markup=kb.as_markup())
        except: pass
        await asyncio.sleep(0.3)
    action_kb = InlineKeyboardBuilder()
    action_kb.row(
        types.InlineKeyboardButton(text="👁 Preview Deletions", callback_data=f"dlt_preview_{album['album_id']}"),
        types.InlineKeyboardButton(text="💾 Save Changes", callback_data=f"dlt_save_{album['album_id']}"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="dlt_cancel")
    )
    await message.answer("⬆️ **✅ Keep** pe click karein delete karne ke liye.\nPhir **Save Changes** dabayein.", reply_markup=action_kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("dlt_toggle_"))
async def dlt_toggle(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid].get("mode") != "dlt":
        return await callback.answer("Session expire ho gaya.", show_alert=True)
    parts = callback.data.split("_")
    idx = int(parts[-2])
    session = user_sessions[uid]
    if idx in session["selected"]:
        session["selected"].discard(idx)
        new_btn, new_cb = "✅ Keep", f"dlt_toggle_{session['album_id']}_{idx}_keep"
    else:
        session["selected"].add(idx)
        new_btn, new_cb = "❌ Delete", f"dlt_toggle_{session['album_id']}_{idx}_del"
    kb = InlineKeyboardBuilder()
    kb.button(text=new_btn, callback_data=new_cb)
    try:
        await callback.message.edit_reply_markup(reply_markup=kb.as_markup())
    except: pass
    await callback.answer()

@dp.callback_query(F.data.startswith("dlt_preview_"))
async def dlt_preview(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid].get("mode") != "dlt":
        return await callback.answer("Session expire.", show_alert=True)
    session = user_sessions[uid]
    if not session["selected"]:
        return await callback.answer("Koi file select nahi ki.", show_alert=True)
    del_nums = sorted([i+1 for i in session["selected"]])
    keep_nums = sorted([i+1 for i in range(len(session["files"])) if i not in session["selected"]])
    await callback.answer()
    await callback.message.answer(
        f"👁 **Delete Preview**\n\n❌ Delete hongi: {', '.join(map(str, del_nums))}\n✅ Raheingi: {', '.join(map(str, keep_nums))}",
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("dlt_save_"))
async def dlt_save(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid].get("mode") != "dlt":
        return await callback.answer("Session expire.", show_alert=True)
    session = user_sessions[uid]
    if not session["selected"]:
        return await callback.answer("Koi file select nahi ki.", show_alert=True)
    del_nums = sorted([i+1 for i in session["selected"]])
    keep_nums = sorted([i+1 for i in range(len(session["files"])) if i not in session["selected"]])
    kb = InlineKeyboardBuilder()
    kb.row(
        types.InlineKeyboardButton(text="🗑️ Haan, Delete Karo", callback_data=f"dlt_confirm_{session['album_id']}"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="dlt_cancel")
    )
    await callback.answer()
    await callback.message.answer(
        f"⚠️ **Delete Confirmation**\n\n📁 Album: **{session['album_name']}**\n"
        f"❌ Delete: File {', '.join(map(str, del_nums))}\n✅ Raheingi: File {', '.join(map(str, keep_nums))}\n\nKya aap sure hain? Yeh action **undo nahi** ho sakta!",
        reply_markup=kb.as_markup(), parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("dlt_confirm_"))
async def dlt_confirm(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid].get("mode") != "dlt":
        return await callback.answer("Session expire.", show_alert=True)
    session = user_sessions[uid]
    new_files = [f for i, f in enumerate(session["files"]) if i not in session["selected"]]
    del_count = len(session["selected"])
    photos, videos, docs, audios = count_media(new_files)
    await albums_col.update_one(
        {"album_id": session["album_id"]},
        {
            "$set": {"photos": new_files, "count": len(new_files), "updated_at": now_db(),
                     "media_count": {"photos": photos, "videos": videos, "docs": docs, "audios": audios}},
            "$push": {"history": {"action": "deleted", "count": -del_count, "by": uid, "at": now_db()}}
        }
    )
    del user_sessions[uid]
    await callback.answer("🗑️ Done!")
    await callback.message.edit_text(
        f"✅ **{del_count} files delete ho gayi!**\n📁 Album: **{session['album_name']}**\n🗂 Remaining: {len(new_files)} files",
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "dlt_cancel")
async def dlt_cancel(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid in user_sessions and user_sessions[uid].get("mode") == "dlt":
        del user_sessions[uid]
    await callback.answer("❌ Cancel")
    await callback.message.edit_text("❌ Delete operation cancel.", parse_mode="Markdown")


# ============================================================
# /merge
# ============================================================
@dp.message(Command("merge"))
async def cmd_merge(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/merge <name/id1> <name/id2> <NewName>`\n"
                                    "Quoted names ke liye: `/merge \'My Pic\' \'Tag Test\' NewAlbum`", parse_mode="Markdown")
    raw = args[1].strip()
    quoted = re.findall(r"['\"](.*?)['\"]+", raw)
    if len(quoted) >= 2:
        id1 = quoted[0].strip()
        id2 = quoted[1].strip()
        second_quote_end = raw.rfind(quoted[1]) + len(quoted[1]) + 1
        new_name = raw[second_quote_end:].strip().strip("'\"")
        if not new_name:
            return await message.answer("❌ New album ka naam dein.", parse_mode="Markdown")
    else:
        tokens = raw.split()
        if len(tokens) < 3:
            return await message.answer("❌ Usage: `/merge <name/id1> <name/id2> <NewName>`", parse_mode="Markdown")
        found = False
        for split1 in range(1, len(tokens) - 1):
            id1_try = " ".join(tokens[:split1])
            a1_try = await find_album(id1_try)
            if not a1_try: continue
            for split2 in range(split1 + 1, len(tokens)):
                id2_try = " ".join(tokens[split1:split2])
                a2_try = await find_album(id2_try)
                if not a2_try: continue
                new_name = " ".join(tokens[split2:])
                if new_name:
                    id1, id2 = id1_try, id2_try
                    found = True
                    break
            if found: break
        if not found:
            return await message.answer("❌ Albums nahi mile. Quoted names use karein:\n"
                                        "`/merge \'My Pic\' \'Tag Test\' NewAlbum`", parse_mode="Markdown")
    a1 = await find_album(id1)
    a2 = await find_album(id2)
    if not a1: return await message.answer(f"❌ Album 1 '{id1}' nahi mila.", parse_mode="Markdown")
    if not a2: return await message.answer(f"❌ Album 2 '{id2}' nahi mila.", parse_mode="Markdown")
    conflict = await albums_col.find_one({"name": {"$regex": f"^{re.escape(new_name)}$", "$options": "i"}})
    if conflict: return await message.answer(f"⚠️ **'{new_name}'** already exists!", parse_mode="Markdown")
    merged_files = a1.get("photos", []) + a2.get("photos", [])
    photos, videos, docs, audios = count_media(merged_files)
    new_id = f"ALB-{now_ist().strftime('%y%m%d%H%M%S')}"
    await albums_col.insert_one({
        "album_id": new_id, "name": new_name, "photos": merged_files, "count": len(merged_files),
        "locked": False, "tags": auto_generate_tags(new_name), "created_by": message.from_user.id,
        "created_at": now_db(), "updated_at": now_db(),
        "history": [{"action": "merged", "from": [a1["album_id"], a2["album_id"]], "by": message.from_user.id, "at": now_db()}],
        "media_count": {"photos": photos, "videos": videos, "docs": docs, "audios": audios}
    })
    await message.answer(
        f"✅ **Albums Merged!**\n\n📁 **{a1['name']}** ({a1['count']}) + **{a2['name']}** ({a2['count']})\n➡️ **{new_name}** | 🆔 `{new_id}`\n🗂 Total: {len(merged_files)} files",
        parse_mode="Markdown"
    )


# ============================================================
# /tag
# ============================================================
@dp.message(Command("tag"))
async def cmd_tag(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/tag <name/id> #tag1 #tag2`", parse_mode="Markdown")
    text = args[1].strip()
    tag_match = re.search(r"#\w+", text)
    if not tag_match: return await message.answer("❌ Koi valid tag nahi mila. Use `#tagname`", parse_mode="Markdown")
    album_identifier = text[:tag_match.start()].strip()
    if not album_identifier: return await message.answer("❌ Album name/id dein.", parse_mode="Markdown")
    album = await find_album(album_identifier)
    if not album: return await message.answer(f"❌ Album '{album_identifier}' nahi mila.", parse_mode="Markdown")
    new_tags = re.findall(r"#\w+", text[tag_match.start():])
    if not new_tags: return await message.answer("❌ Koi valid tag nahi mila. Use `#tagname`", parse_mode="Markdown")
    existing_tags = album.get("tags", [])
    all_tags = list(set(existing_tags + [t.lower() for t in new_tags]))
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"tags": all_tags, "updated_at": now_db()}})
    await message.answer(f"🏷️ **Tags Updated!**\n📁 **{album['name']}**\nTags: {' '.join(all_tags)}", parse_mode="Markdown")


# ============================================================
# /albums
# ============================================================
@dp.message(Command("albums"))
async def cmd_list(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    try:
        albums = await albums_col.find().sort("created_at", -1).to_list(length=50)
        if not albums:
            return await message.answer("📂 Cloud empty hai! /album se banayein.")

        total_files = sum(a.get("count", 0) for a in albums)
        locked_count = sum(1 for a in albums if a.get("locked"))

        # Sab ek message mein — sirf naam + ID
        lines = (
            f"☁️ *Personal Cloud*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📊 {len(albums)} albums  🗂 {total_files} files  🔒 {locked_count} locked\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
        )
        for alb in albums:
            icon = "🔒" if alb.get("locked") else "📁"
            aid  = alb.get("album_id") or "N/A"
            name = alb.get("name") or "Unnamed"
            lines += f"{icon} {name}\n🆔 `{aid}`\n\n"

        await message.answer(lines.strip(), parse_mode="Markdown")

    except Exception as e:
        logger.error(f"/albums error: {e}")
        await message.answer(f"❌ Error: {e}")


# ============================================================
# /info
# ============================================================
@dp.message(Command("info"))
async def cmd_info(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: /info AlbumName  ya  /info ALB-xxx")
    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.")
    mc = album.get("media_count", {})
    photos = mc.get("photos", 0)
    videos = mc.get("videos", 0)
    docs   = mc.get("docs", 0)
    audios = mc.get("audios", 0)
    if not mc:
        photos, videos, docs, audios = count_media(album.get("photos", []))
    aid  = album["album_id"]
    tags = " ".join(album.get("tags", [])) or None
    lock = "🔒 Locked" if album.get("locked") else "🔓 Unlocked"

    raw_created = album.get("created_at", now_db())
    if raw_created.tzinfo is None:
        from datetime import timezone
        raw_created = raw_created.replace(tzinfo=timezone.utc)
    created = raw_created.astimezone(IST).strftime("%d %b %Y, %I:%M %p") + " IST"

    by_username = album.get("created_by_username", "")
    by_str = f"@{by_username}" if by_username else f"`{album.get('created_by', 'N/A')}`"

    files_list = album.get("photos", [])
    total_size_bytes = sum(f.get("file_size", 0) for f in files_list if isinstance(f, dict))
    if total_size_bytes > 0:
        if total_size_bytes >= 1024 * 1024 * 1024:
            size_str = f"{total_size_bytes / (1024**3):.1f} GB"
        elif total_size_bytes >= 1024 * 1024:
            size_str = f"{total_size_bytes / (1024**2):.1f} MB"
        else:
            size_str = f"{total_size_bytes / 1024:.0f} KB"
    else:
        est = (photos * 2 + videos * 50 + docs * 5 + audios * 4)
        size_str = f"~{est} MB" if est < 1024 else f"~{est/1024:.1f} GB"

    text = (
        f"📋 Album Info\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📁 Name: {album['name']}\n"
        f"🆔 `{aid}`\n"
        f"👁 `/view {aid}`\n"
        f"📦 `/zip {aid}`\n"
        f"👤 {by_str}\n"
        f"📅 {created}\n"
        f"🔐 {lock}\n"
        f"\n🗂 Files:\n"
    )
    if photos: text += f"📸 Photos: {photos}\n"
    if videos: text += f"🎥 Videos: {videos}\n"
    if docs:   text += f"📄 Documents: {docs}\n"
    if audios: text += f"🎵 Audio: {audios}\n"
    text += f"📊 Total: {album['count']}\n"
    text += f"💾 Size: {size_str}\n"

    history = album.get("history", [])
    if history:
        text += "\n📜 History:\n"
        for h in history[-5:]:
            action   = h.get("action", "")
            count    = h.get("count", 0)
            at       = h.get("at", now_db())
            date_str = at.strftime("%d %b %Y") if isinstance(at, datetime) else str(at)
            if action == "created":  text += f"   Created | {date_str}\n"
            elif action == "added":  text += f"   +{count} files | {date_str}\n"
            elif action == "deleted":text += f"   -{abs(count)} files | {date_str}\n"
            elif action == "merged": text += f"   Merged | {date_str}\n"

    if tags:
        text += f"\n🏷️ Tags: {tags}"

    await message.answer(text, parse_mode="Markdown")


# ============================================================
# /view
# ============================================================
@dp.message(F.text.regexp(r"^/view_[A-Za-z0-9\-]+$"))
async def view_shortcut(message: types.Message):
    aid = message.text.replace("/view_", "").strip()
    message.text = f"/view {aid}"
    await view_by_id(message)

@dp.message(Command("view"))
async def view_by_id(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage:\n/view AlbumName\n/view ALB-xxx\n/view #tag1 #tag2")

    identifier = args[1].strip()

    tags_input = [w.lower() for w in identifier.split() if w.startswith("#")]
    if tags_input:
        query_conditions = []
        for tag in tags_input:
            query_conditions.append({"tags": {"$elemMatch": {"$regex": f"^{re.escape(tag)}", "$options": "i"}}})

        cursor = albums_col.find({"$and": query_conditions} if len(query_conditions) > 1 else query_conditions[0]).sort("created_at", -1)
        results = await cursor.to_list(length=50)

        if not results:
            return await message.answer(f"❌ '{identifier}' se koi album nahi mila.")

        await message.answer(f"🏷️ {identifier} — {len(results)} album(s) mila")

        for alb in results:
            icon  = "🔒" if alb.get("locked") else "🔓"
            aid   = alb["album_id"]
            name  = alb.get("name", "Unnamed")
            date  = alb.get("created_at", now_db())
            if date.tzinfo is None:
                from datetime import timezone
                date = date.replace(tzinfo=timezone.utc)
            date_str = date.astimezone(IST).strftime("%d %b %Y, %I:%M %p")
            album_tags = "  ".join(alb.get("tags", []))
            mc  = alb.get("media_count", {})
            p = mc.get("photos",0); v = mc.get("videos",0)
            d = mc.get("docs",0);   a = mc.get("audios",0)
            tp = []
            if p: tp.append(f"📸 {p}")
            if v: tp.append(f"🎥 {v}")
            if d: tp.append(f"📄 {d}")
            if a: tp.append(f"🎵 {a}")
            type_str = "  ".join(tp) if tp else f"🗂 {alb.get('count',0)}"
            lock_status = "🔒 Locked" if alb.get("locked") else "🔓 Unlocked"

            icon = "🔒" if alb.get("locked") else "📁"
            card = f"{icon} {name}\n🆔 `{aid}`\n👁 `/view {aid}`"
            if album_tags: card += f"\n\n🏷️ {album_tags}"

            await message.answer(card, parse_mode="Markdown")
            await asyncio.sleep(0.05)
        return

    album = await find_album(identifier)
    if not album:
        return await message.answer(f"❌ Album '{identifier}' nahi mila.")

    # ── Password check (only for non-owner) ──────────────────
    uid = message.from_user.id
    album_pass = album.get("password")
    if album_pass and not is_owner(uid):
        password_pending[uid] = {"action": "view", "album": album}
        return await message.answer(
            f"🔐 *{album['name']}* password protected hai!\n\nPassword bhejein:",
            parse_mode="Markdown"
        )

    mc = album.get("media_count", {})
    p = mc.get("photos",0); v = mc.get("videos",0)
    d = mc.get("docs",0);   a = mc.get("audios",0)
    tp = []
    if p: tp.append(f"📸{p}")
    if v: tp.append(f"🎥{v}")
    if d: tp.append(f"📄{d}")
    if a: tp.append(f"🎵{a}")
    type_str = "  ".join(tp) if tp else f"{album['count']} files"
    await message.answer(
        f"📂 {album['name']}\n🆔 {album['album_id']}\n🗂 {type_str}\nLoading...\n\n⏹ Rokna ho toh `/close` likhein"
    )
    view_sessions[uid] = True
    files = album.get("photos", [])
    sent = failed = 0
    for item in files:
        # /close se stop check
        if not view_sessions.get(uid):
            await message.answer(f"⏹ View band kar diya.\n✅ {sent} files bhej chuke the.")
            return
        fid = item["file_id"] if isinstance(item, dict) else item
        mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
        channel_msg_id = item.get("channel_msg_id") if isinstance(item, dict) else None
        try:
            if channel_msg_id:
                await bot.forward_message(message.chat.id, STORAGE_CHANNEL, channel_msg_id)
            elif mtype == "video":    await bot.send_video(message.chat.id, fid)
            elif mtype == "document": await bot.send_document(message.chat.id, fid)
            elif mtype == "audio":    await bot.send_audio(message.chat.id, fid)
            else:                     await bot.send_photo(message.chat.id, fid)
            sent += 1
        except: failed += 1
        await asyncio.sleep(0.3)
    view_sessions.pop(uid, None)
    summary = f"✅ {sent}/{len(files)} files sent!"
    if failed: summary += f"\n⚠️ {failed} failed."
    await message.answer(summary)


# ============================================================
# /zip  —  Smart Export
# ============================================================
@dp.message(F.text.regexp(r"^/zip_[A-Za-z0-9\-]+$"))
async def zip_shortcut(message: types.Message):
    aid = message.text.replace("/zip_", "").strip()
    message.text = f"/zip {aid}"
    await cmd_zip(message)

@dp.message(Command("zip"))
async def cmd_zip(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/zip AlbumName` ya `/zip ALB-xxx`", parse_mode="Markdown")

    album = await find_album(args[1].strip())
    if not album:
        return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")

    # ── Password check (only for non-owner) ──────────────────
    zip_uid = message.from_user.id
    album_pass = album.get("password")
    if album_pass and not is_owner(zip_uid):
        password_pending[zip_uid] = {"action": "zip", "album": album}
        return await message.answer(
            f"🔐 *{album['name']}* password protected hai!\n\nPassword bhejein:",
            parse_mode="Markdown"
        )

    files = album.get("photos", [])
    if not files:
        return await message.answer("❌ Album empty hai.", parse_mode="Markdown")

    DOWNLOAD_LIMIT = 20 * 1024 * 1024
    SPLIT_SIZE     = 45 * 1024 * 1024
    EXT_MAP = {"photo": "jpg", "video": "mp4", "document": "bin", "audio": "mp3", "voice": "ogg"}

    status_msg = await message.answer(
        f"🔍 Files check kar raha hoon...\n"
        f"📁 **{album['name']}** | 🗂 {len(files)} files",
        parse_mode="Markdown"
    )

    small_files = []
    large_files = []
    check_failed = 0

    for idx, item in enumerate(files, 1):
        fid            = item["file_id"]            if isinstance(item, dict) else item
        mtype          = item.get("type",  "photo") if isinstance(item, dict) else "photo"
        fname          = item.get("name",  "")      if isinstance(item, dict) else ""
        channel_msg_id = item.get("channel_msg_id") if isinstance(item, dict) else None

        try:
            tg_file = await bot.get_file(fid)
            fsize   = tg_file.file_size or 0
            if fsize < DOWNLOAD_LIMIT:
                small_files.append((fid, mtype, fname, tg_file))
            else:
                large_files.append((fid, mtype, fname, channel_msg_id))
        except Exception:
            if channel_msg_id:
                large_files.append((fid, mtype, fname, channel_msg_id))
            else:
                check_failed += 1

        if idx % 20 == 0:
            try:
                await status_msg.edit_text(
                    f"🔍 Checking... {idx}/{len(files)}\n📁 **{album['name']}**",
                    parse_mode="Markdown"
                )
            except: pass

    try:
        await status_msg.edit_text(
            f"📊 **{album['name']}**\n"
            f"📦 ZIP (<20 MB): {len(small_files)} files\n"
            f"📤 Direct forward (≥20 MB): {len(large_files)} files\n"
            f"{'⚠️ Unreachable: ' + str(check_failed) + ' files' + chr(10) if check_failed else ''}"
            f"⏳ Processing...",
            parse_mode="Markdown"
        )
    except: pass

    zip_parts_sent = 0
    total_zipped   = 0
    forwarded      = 0
    fwd_failed     = 0

    if small_files:
        try:
            await status_msg.edit_text(
                f"⏬ Downloading {len(small_files)} files...\n📁 **{album['name']}**",
                parse_mode="Markdown"
            )
        except: pass

        downloaded = []

        for idx, (fid, mtype, fname, tg_file) in enumerate(small_files, 1):
            try:
                url = f"https://api.telegram.org/file/bot{API_TOKEN}/{tg_file.file_path}"
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            ext = (tg_file.file_path.rsplit(".", 1)[-1]
                                   if "." in tg_file.file_path
                                   else EXT_MAP.get(mtype, "bin"))
                            safe_name = fname if fname else f"{idx:04d}_{mtype}.{ext}"
                            downloaded.append((safe_name, data))
            except Exception as e:
                logger.error(f"Download error idx {idx}: {e}")

            if idx % 10 == 0:
                try:
                    await status_msg.edit_text(
                        f"⏬ Downloading... {idx}/{len(small_files)}\n📁 **{album['name']}**",
                        parse_mode="Markdown"
                    )
                except: pass

        if downloaded:
            try:
                await status_msg.edit_text(
                    f"🗜 Packing ZIP ({len(downloaded)} files)...\n📁 **{album['name']}**",
                    parse_mode="Markdown"
                )
            except: pass

            zip_name = re.sub(r'[^\w\s\-]', '', album["name"]).strip().replace(' ', '_') or "album"
            parts     = []
            cur_buf   = io.BytesIO()
            cur_zf    = zipfile.ZipFile(cur_buf, mode='w', compression=zipfile.ZIP_DEFLATED)
            cur_size  = 0
            cur_count = 0

            for safe_name, data in downloaded:
                if cur_size + len(data) > SPLIT_SIZE and cur_count > 0:
                    cur_zf.close()
                    cur_buf.seek(0)
                    parts.append((cur_buf, cur_count))
                    cur_buf   = io.BytesIO()
                    cur_zf    = zipfile.ZipFile(cur_buf, mode='w', compression=zipfile.ZIP_DEFLATED)
                    cur_size  = 0
                    cur_count = 0

                cur_zf.writestr(safe_name, data)
                cur_size  += len(data)
                cur_count += 1

            if cur_count > 0:
                cur_zf.close()
                cur_buf.seek(0)
                parts.append((cur_buf, cur_count))

            total_parts = len(parts)

            for part_num, (zip_buf, file_count) in enumerate(parts, 1):
                part_fname = (
                    f"{zip_name}_part{part_num}.zip" if total_parts > 1
                    else f"{zip_name}.zip"
                )
                part_label = f" (Part {part_num}/{total_parts})" if total_parts > 1 else ""

                try:
                    await bot.send_document(
                        message.chat.id,
                        document=types.BufferedInputFile(zip_buf.read(), filename=part_fname),
                        caption=f"📦 {part_fname}{part_label}\n🗂 {file_count} files | Saved"
                    )
                    zip_parts_sent += 1
                    total_zipped   += file_count
                except Exception as e:
                    logger.error(f"ZIP send error part {part_num}: {e}")
                    await message.answer(f"ZIP Part {part_num} send nahi hua: {e}")

    if large_files:
        await message.answer(
            f"📤 **{len(large_files)} badi file(s)** direct bhej raha hoon (≥20 MB)...",
            parse_mode="Markdown"
        )
        for fid, mtype, fname, channel_msg_id in large_files:
            try:
                if channel_msg_id:
                    await bot.forward_message(message.chat.id, STORAGE_CHANNEL, channel_msg_id)
                elif mtype == "video":
                    await bot.send_video(message.chat.id, fid)
                elif mtype == "document":
                    await bot.send_document(message.chat.id, fid)
                elif mtype == "audio":
                    await bot.send_audio(message.chat.id, fid)
                elif mtype == "voice":
                    await bot.send_voice(message.chat.id, fid)
                else:
                    await bot.send_photo(message.chat.id, fid)
                forwarded += 1
            except Exception as e:
                logger.error(f"Large file forward error: {e}")
                fwd_failed += 1
            await asyncio.sleep(0.4)

    final = f"✅ **Done! — {album['name']}**\n\n"
    if zip_parts_sent:
        final += f"📦 ZIP: {zip_parts_sent} part(s) | {total_zipped} files packed\n"
    if forwarded:
        final += f"📤 Direct forwarded: {forwarded} large file(s)\n"
    if fwd_failed:
        final += f"⚠️ Forward failed: {fwd_failed}\n"
    if check_failed:
        final += f"❌ Unreachable: {check_failed} files\n"
    if zip_parts_sent == 0 and forwarded == 0:
        final = "❌ Koi bhi file process nahi ho saki."

    await status_msg.edit_text(final, parse_mode="Markdown")


# ============================================================
# /stats
# ============================================================
@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    try:
        total = await albums_col.count_documents({})
        locked = await albums_col.count_documents({"locked": True})
        pipeline = [{"$group": {"_id": None, "total": {"$sum": "$count"}}}]
        res = await albums_col.aggregate(pipeline).to_list(1)
        total_files = res[0]["total"] if res else 0
        latest = await albums_col.find_one(sort=[("created_at", -1)])
        largest = await albums_col.find_one(sort=[("count", -1)])
        await message.answer(
            f"📊 **Cloud Stats**\n━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📁 Albums: {total}\n🗂 Total Files: {total_files}\n"
            f"🔒 Locked: {locked} | 🔓 Unlocked: {total-locked}\n\n"
            f"📅 Latest: **{latest['name'] if latest else '-'}**\n"
            f"🏆 Largest: **{largest['name']} ({largest['count']} files)**\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n🟢 Bot: Online\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
            parse_mode="Markdown"
        )
    except Exception as e:
        await message.answer(f"❌ Stats error: `{e}`", parse_mode="Markdown")


# ============================================================
# /b2 - Send album to user(s)
# ============================================================
@dp.message(Command("b2"))
async def cmd_b2(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/b2 <id\/name> @u1 @u2` ya `/b2 <id\/name> userid`", parse_mode="Markdown")
    text = args[1].strip()
    tokens = text.split()
    if len(tokens) < 2: return await message.answer("❌ Album name/id aur recipient dein.", parse_mode="Markdown")
    targets_raw = []
    name_tokens = []
    for t in reversed(tokens):
        if t.startswith("@") or t.lstrip("-").isdigit():
            targets_raw.insert(0, t)
        else:
            name_tokens = tokens[:tokens.index(t) + 1]
            break
    if not targets_raw: return await message.answer("❌ Recipient (@user ya userid) dein.", parse_mode="Markdown")
    if not name_tokens: return await message.answer("❌ Album name/id dein.", parse_mode="Markdown")
    album_identifier = " ".join(name_tokens)
    album = await find_album(album_identifier)
    if not album: return await message.answer(f"❌ Album '{album_identifier}' nahi mila.", parse_mode="Markdown")
    files = album.get("photos", [])
    if not files: return await message.answer("❌ Album empty hai.", parse_mode="Markdown")
    target_ids = []
    for t in targets_raw:
        if t.lstrip("-").isdigit():
            target_ids.append((int(t), t))
        elif t.startswith("@"):
            uname = t.lstrip("@").lower()
            doc = await db.granted_users.find_one({"username": uname})
            if doc and doc.get("user_id"): target_ids.append((doc["user_id"], t))
            else: await message.answer(f"⚠️ {t} ka ID nahi mila, skip.", parse_mode="Markdown")
    if not target_ids: return await message.answer("❌ Koi valid recipient nahi mila.", parse_mode="Markdown")
    await message.answer(f"📤 Sending **{album['name']}** to {len(target_ids)} user(s)...", parse_mode="Markdown")
    for uid, uname in target_ids:
        try:
            await bot.send_message(uid, f"📂 **{album['name']}**\n🗂 {len(files)} files\n_Loading..._", parse_mode="Markdown")
            sent = 0
            for item in files:
                fid = item["file_id"] if isinstance(item, dict) else item
                mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
                try:
                    if mtype == "video": await bot.send_video(uid, fid)
                    elif mtype == "document": await bot.send_document(uid, fid)
                    elif mtype == "audio": await bot.send_audio(uid, fid)
                    else: await bot.send_photo(uid, fid)
                    sent += 1
                except: pass
                await asyncio.sleep(0.3)
            await bot.send_message(uid, f"✅ **{sent} files** received!", parse_mode="Markdown")
            await b2_history_col.insert_one({
                "album_id": album["album_id"], "album_name": album["name"],
                "sent_by": message.from_user.id, "sent_to": uid, "sent_to_name": uname,
                "files_count": sent, "sent_at": now_db()
            })
            await message.answer(f"✅ **{uname}** ko {sent} files bhej di!", parse_mode="Markdown")
        except Exception as e:
            await message.answer(f"❌ **{uname}** ko bhejne mein error: {e}", parse_mode="Markdown")


# ============================================================



# ============================================================
# /makelist  — Pehli baar checklist banana
# ============================================================
@dp.message(Command("makelist"))
async def cmd_makelist(message: types.Message):
    if not is_owner(message.from_user.id): return await message.answer("🚫 Sirf owner!")
    args = message.text.split(maxsplit=1)
    title = args[1].strip() if len(args) > 1 else "B2 CLOUD"

    # Title save karo
    await db.settings.update_one(
        {"key": "checklist_title"},
        {"$set": {"key": "checklist_title", "value": title}},
        upsert=True
    )

    # Check if already exists
    existing = await db.settings.find_one({"key": "checklist_msg_id"})
    if existing:
        return await message.answer(
            f"⚠️ Checklist already exist karta hai!\n"
            f"Message ID: `{existing['value']}`\n\n"
            f"Update karna hai toh `/makelist` se pehle `/removelist` karo.",
            parse_mode="Markdown"
        )

    # Build and send checklist to storage channel
    checklist_text = await rebuild_checklist_text()
    try:
        sent = await bot.send_message(
            STORAGE_CHANNEL,
            checklist_text,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        # Pin it
        await bot.pin_chat_message(STORAGE_CHANNEL, sent.message_id, disable_notification=True)
        # Save msg_id
        await db.settings.update_one(
            {"key": "checklist_msg_id"},
            {"$set": {"key": "checklist_msg_id", "value": sent.message_id}},
            upsert=True
        )
        await message.answer(
            f"✅ **Checklist create ho gaya!**\n"
            f"📌 Pinned in storage channel\n"
            f"🏷️ Title: **{title}**\n"
            f"🆔 Message ID: `{sent.message_id}`",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"makelist error: {e}")
        await message.answer(f"❌ Error: {e}")


@dp.message(Command("removelist"))
async def cmd_removelist(message: types.Message):
    if not is_owner(message.from_user.id): return await message.answer("🚫 Sirf owner!")
    setting = await db.settings.find_one({"key": "checklist_msg_id"})
    if not setting:
        return await message.answer("⚠️ Koi checklist nahi mila.")
    try:
        await bot.unpin_chat_message(STORAGE_CHANNEL, setting["value"])
        await bot.delete_message(STORAGE_CHANNEL, setting["value"])
    except: pass
    await db.settings.delete_one({"key": "checklist_msg_id"})
    await message.answer("🗑️ Checklist remove ho gaya!")


# ============================================================
# /setpass  &  /removepass
# ============================================================
@dp.message(Command("setpass"))
async def cmd_setpass(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.answer(
            "❌ Usage: `/setpass <album name/id> <password>`\n"
            "Example: `/setpass MyTrip secret123`",
            parse_mode="Markdown"
        )
    identifier = args[1].strip()
    password   = args[2].strip()
    album = await find_album(identifier)
    if not album: return await message.answer(f"❌ Album '{identifier}' nahi mila.", parse_mode="Markdown")
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"password": password, "updated_at": now_db()}})
    await message.answer(
        f"🔐 Password set!\n📁 **{album['name']}**\n🔑 `{password}`\n\n"
        f"Ab `/view` ya `/zip` karte waqt granted users se password maanga jayega.",
        parse_mode="Markdown"
    )

@dp.message(Command("removepass"))
async def cmd_removepass(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/removepass <album name/id>`", parse_mode="Markdown")
    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")
    if not album.get("password"):
        return await message.answer(f"⚠️ **{album['name']}** pe koi password nahi tha.", parse_mode="Markdown")
    await albums_col.update_one({"_id": album["_id"]}, {"$unset": {"password": ""}, "$set": {"updated_at": now_db()}})
    await message.answer(f"🔓 Password remove ho gaya!\n📁 **{album['name']}**", parse_mode="Markdown")


# ============================================================
# PASSWORD INPUT HANDLER
# ============================================================
@dp.message(F.text & ~F.text.startswith("/"))
async def handle_password_input(message: types.Message):
    uid = message.from_user.id
    if uid not in password_pending: return
    if uid in user_sessions: return   # album session chal raha hai — ignore

    pending = password_pending[uid]
    album   = pending["album"]
    action  = pending["action"]

    # Re-fetch latest album from DB for fresh password
    fresh = await albums_col.find_one({"_id": album["_id"]})
    if not fresh:
        del password_pending[uid]
        return

    entered  = message.text.strip()
    correct  = fresh.get("password", "")

    if entered != correct:
        return await message.answer("❌ Wrong password! Dobara try karein:")

    # ── Correct — proceed ────────────────────────────────────
    del password_pending[uid]

    if action == "view":
        # Trigger view with correct album
        message.text = f"/view {fresh['album_id']}"
        await view_by_id(message)

    elif action == "zip":
        message.text = f"/zip {fresh['album_id']}"
        await cmd_zip(message)


# ============================================================
# /cancel
# ============================================================
@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message):
    uid = message.from_user.id
    if uid in user_sessions:
        session = user_sessions[uid]
        del user_sessions[uid]
        await message.answer(
            f"❌ **Session Cancel!**\nMode: {session.get('mode')} | Album: {session.get('name', '')}\n_{len(session.get('photos', []))} unsaved files discard ho gayi._",
            parse_mode="Markdown"
        )
    else:
        await message.answer("⚠️ Koi active session nahi.")


# ============================================================
# GRANT SYSTEM
# ============================================================
async def send_greeting(user_id: int, fallback_name: str = "Friend"):
    try:
        try:
            uc = await bot.get_chat(user_id)
            name = uc.first_name or fallback_name
        except: name = fallback_name
        now = now_db()
        await bot.send_message(user_id,
            f"👋 **HEY {name}!**\n\n🎉 **Grant Access Successfully!**\n\n🥳 **ENJOY!!**\n\n"
            f"📅 **Access Date:** {now.strftime('%d %B %Y')}\n🕐 **Access Time:** {now.strftime('%I:%M %p')} IST",
            parse_mode="Markdown"
        )
        return True
    except Exception as e:
        logger.warning(f"Greeting failed: {e}")
        return False

@dp.message(Command("grant"))
async def cmd_grant(message: types.Message):
    if not is_owner(message.from_user.id): return await message.answer("🚫 Sirf owner!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage:\n`/grant 123456789`\n`/grant @username`", parse_mode="Markdown")
    target = args[1].strip()
    if target.lstrip("-").isdigit():
        uid = int(target)
        if uid == ADMIN_ID: return await message.answer("⚠️ Aap owner hain already!")
        granted_users.add(uid)
        fetched_username = None
        fetched_fullname = None
        try:
            chat = await bot.get_chat(uid)
            fetched_username = chat.username.lower() if chat.username else None
            fetched_fullname = chat.full_name if hasattr(chat, "full_name") else None
        except: pass
        await db.granted_users.update_one(
            {"user_id": uid},
            {"$set": {"user_id": uid, "username": fetched_username, "full_name": fetched_fullname, "granted_at": now_db(), "granted_by": message.from_user.id}},
            upsert=True
        )
        uname_str = f"@{fetched_username}" if fetched_username else f"ID: {uid}"
        await message.answer(f"✅ Access Granted!\n👤 {uname_str}\n🆔 {uid}")
        ok = await send_greeting(uid)
        if not ok: await message.answer("⚠️ User ko greeting nahi gayi — user ne pehle /start kiya ho.")
    elif target.startswith("@"):
        username = target.lstrip("@").lower()
        doc = await db.granted_users.find_one({"username": username})
        if doc and doc.get("user_id"):
            uid = doc["user_id"]
            granted_users.add(uid)
            await db.granted_users.update_one({"user_id": uid}, {"$set": {"granted_at": now_db(), "granted_by": message.from_user.id}}, upsert=True)
            await message.answer(f"✅ **Access Granted!**\n👤 @{username} | 🆔 `{uid}`", parse_mode="Markdown")
            ok = await send_greeting(uid, username)
            if not ok: await message.answer("⚠️ User ko greeting nahi gayi.", parse_mode="Markdown")
        else:
            await db.granted_users.update_one(
                {"username": username},
                {"$set": {"username": username, "user_id": None, "granted_at": now_db(), "granted_by": message.from_user.id, "pending": True}},
                upsert=True
            )
            await message.answer(f"⏳ **Pending Grant!**\n👤 @{username}\nJab pehli baar /start karenge, activate ho jayega.\n💡 User ID zyada reliable hai.", parse_mode="Markdown")
    else:
        await message.answer("❌ Valid User ID ya @username dein.", parse_mode="Markdown")

@dp.message(Command("denied"))
async def cmd_denied(message: types.Message):
    if not is_owner(message.from_user.id): return await message.answer("🚫 Sirf owner!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/denied 123` ya `/denied @user`", parse_mode="Markdown")
    target = args[1].strip()
    if target.lstrip("-").isdigit():
        uid = int(target)
        if uid == ADMIN_ID: return await message.answer("⚠️ Owner ka access nahi hata sakte!")
        granted_users.discard(uid)
        doc = await db.granted_users.find_one({"user_id": uid})
        r = await db.granted_users.delete_one({"user_id": uid})
        if r.deleted_count:
            uname_saved = doc.get("username") if doc else None
            await db.denied_users.update_one(
                {"user_id": uid},
                {"$set": {"user_id": uid, "username": uname_saved, "denied_at": now_db()}},
                upsert=True
            )
            await message.answer(f"🚫 Access removed!\n🆔 `{uid}`", parse_mode="Markdown")
        else: await message.answer(f"⚠️ `{uid}` list mein nahi tha.", parse_mode="Markdown")
    elif target.startswith("@"):
        username = target.lstrip("@").lower()
        doc = await db.granted_users.find_one({"username": username})
        if doc:
            uid_saved = doc.get("user_id")
            if uid_saved: granted_users.discard(uid_saved)
            await db.granted_users.delete_one({"username": username})
            await db.denied_users.update_one(
                {"username": username},
                {"$set": {"user_id": uid_saved, "username": username, "denied_at": now_db()}},
                upsert=True
            )
            await message.answer(f"🚫 @{username} access removed!", parse_mode="Markdown")
        else: await message.answer(f"⚠️ @{username} list mein nahi tha.", parse_mode="Markdown")
    else: await message.answer("❌ Valid ID ya @username dein.", parse_mode="Markdown")

@dp.message(Command("list"))
async def cmd_list_all(message: types.Message):
    if not is_owner(message.from_user.id): return await message.answer("🚫 Sirf owner!")

    # ── 1. LOCKED ALBUMS ─────────────────────────────────────
    albums   = await albums_col.find().sort("name", 1).to_list(200)
    locked   = [a for a in albums if a.get("locked")]
    unlocked = [a for a in albums if not a.get("locked")]

    text = ""

    if locked:
        text += "🔒 *LOCK* (`/lock`)\n"
        for a in locked:
            aid  = a.get("album_id", "N/A")
            name = a.get("name", "Unnamed")
            text += f"📁 {name}\n🆔 `/unlock {aid}`\n\n"
    else:
        text += "🔒 *LOCK* — Koi locked album nahi\n\n"

    # ── 2. UNLOCKED ALBUMS ───────────────────────────────────
    if unlocked:
        text += "🔓 *UNLOCK* (`/unlock`)\n"
        for a in unlocked:
            aid  = a.get("album_id", "N/A")
            name = a.get("name", "Unnamed")
            text += f"📁 {name}\n🆔 `/lock {aid}`\n\n"
    else:
        text += "🔓 *UNLOCK* — Koi unlocked album nahi\n\n"

    # ── 3. GRANTED USERS ─────────────────────────────────────
    granted = await db.granted_users.find().to_list(100)
    text += "👥 *Granted Users:* (`/grant`)\n━━━━━━━━━━━━━━━━━━\n\n"
    if granted:
        for u in granted:
            uid_val  = u.get("user_id")
            uname    = u.get("username")
            fullname = u.get("full_name", "")
            pending  = u.get("pending", False)
            raw_date = u.get("granted_at", now_db())
            if raw_date.tzinfo is None:
                from datetime import timezone
                raw_date = raw_date.replace(tzinfo=timezone.utc)
            date     = raw_date.astimezone(IST).strftime("%d %b %Y, %I:%M %p") + " IST"
            deny_ref = f"@{uname}" if uname else str(uid_val)
            if fullname: text += f"📛 {fullname}\n"
            if uname:    text += f"👤 @{uname}\n"
            text += f"🆔 `{uid_val}`\n"
            text += f"📅 {date}\n"
            if pending:  text += "⏳ Pending\n"
            text += f"`/denied {deny_ref}`\n\n"
        text += f"━━━━━━━━━━━━━━━━━━\nTotal: {len(granted):02d}\n\n"
    else:
        text += "Koi granted user nahi.\n\n"

    # ── 4. DENIED USERS ──────────────────────────────────────
    denied = await db.denied_users.find().sort("denied_at", -1).to_list(100)
    text += "🚫 *Denied Users:* (`/denied`)\n━━━━━━━━━━━━━━━━━━\n\n"
    if denied:
        for u in denied:
            uid_val  = u.get("user_id")
            uname    = u.get("username")
            raw_date = u.get("denied_at", now_db())
            if raw_date.tzinfo is None:
                from datetime import timezone
                raw_date = raw_date.replace(tzinfo=timezone.utc)
            date      = raw_date.astimezone(IST).strftime("%d %b %Y, %I:%M %p") + " IST"
            grant_ref = f"@{uname}" if uname else str(uid_val)
            if uname: text += f"👤 @{uname}\n"
            text += f"🆔 `{uid_val}`\n"
            text += f"📅 {date}\n"
            text += f"`/grant {grant_ref}`\n\n"
        text += f"━━━━━━━━━━━━━━━━━━\nTotal: {len(denied):02d}\n\n"
    else:
        text += "Koi denied user nahi.\n\n"

    # ── 5. SHARE HISTORY ─────────────────────────────────────
    total_b2 = await b2_history_col.count_documents({})
    history  = await b2_history_col.find().sort("sent_at", -1).limit(50).to_list(50)
    text += f"📤 *Share History all ({total_b2}):*\n\n"
    if history:
        for h in history:
            raw_date = h.get("sent_at", now_db())
            if raw_date.tzinfo is None:
                from datetime import timezone
                raw_date = raw_date.replace(tzinfo=timezone.utc)
            date         = raw_date.astimezone(IST).strftime("%d %b %Y, %I:%M %p")
            sent_to_name = h.get("sent_to_name", "")
            sent_to_id   = h.get("sent_to", "")
            if sent_to_name and sent_to_name.startswith("@"):
                recipient = sent_to_name
            elif sent_to_id:
                doc   = await db.granted_users.find_one({"user_id": int(sent_to_id)}) if str(sent_to_id).isdigit() else None
                uname = doc.get("username") if doc else None
                recipient = f"@{uname}" if uname else str(sent_to_id)
            else:
                recipient = "—"
            text += (
                f"📁 {h.get('album_name', 'N/A')}\n"
                f"➡️ To: {recipient}\n"
                f"🗂 {h.get('files_count', 0)} files\n"
                f"📅 {date}\n\n"
            )
    else:
        text += "Koi share history nahi.\n"

    # ── Send (split if too long) ──────────────────────────────
    if len(text) <= 4000:
        await message.answer(text, parse_mode="Markdown")
    else:
        parts = []
        cur = ""
        for line in text.split("\n"):
            if len(cur) + len(line) + 1 > 3800:
                parts.append(cur)
                cur = ""
            cur += line + "\n"
        if cur.strip(): parts.append(cur)
        for p in parts:
            await message.answer(p, parse_mode="Markdown")
            await asyncio.sleep(0.2)


@dp.message(Command("idinfo"))
async def cmd_idinfo(message: types.Message):
    if not is_owner(message.from_user.id): return await message.answer("🚫 Sirf owner!")
    args = message.text.split(maxsplit=1)

    # ── Mode 1: /idinfo (no args) — saare granted users + unke albums ──
    if len(args) < 2:
        granted = await db.granted_users.find({"pending": {"$ne": True}}).to_list(100)
        if not granted:
            return await message.answer("📋 Koi granted user nahi.", parse_mode="Markdown")
        text = "👥 *Granted Users Info:*\n━━━━━━━━━━━━━━━━━━\n\n"
        for u in granted:
            uid_val  = u.get("user_id")
            uname    = u.get("username")
            fullname = u.get("full_name", "")
            raw_date = u.get("granted_at", now_db())
            if raw_date.tzinfo is None:
                from datetime import timezone
                raw_date = raw_date.replace(tzinfo=timezone.utc)
            date = raw_date.astimezone(IST).strftime("%d %b %Y, %I:%M %p") + " IST"
            albums = await albums_col.find({"created_by": uid_val}).sort("created_at", -1).to_list(50)
            if fullname: text += f"📛 {fullname}\n"
            if uname:    text += f"👤 @{uname}\n"
            text += f"🆔 `{uid_val}`\n"
            text += f"📅 Granted: {date}\n"
            if albums:
                text += f"📁 Albums ({len(albums)}):\n"
                for alb in albums:
                    alb_date = alb.get("created_at", now_db()).strftime("%d %b %Y")
                    text += f"   • {alb['name']} | 🗂{alb['count']} | {alb_date}\n"
            else:
                text += "📁 Koi album nahi\n"
            text += "\n━━━━━━━━━━━━━━━━━━\n\n"
        await message.answer(text, parse_mode="Markdown")
        return

    # ── Mode 2: /idinfo <id/@username> — kisi bhi user ka info ──
    target_arg = args[1].strip()
    target_uid = None
    tg_info    = None

    if target_arg.startswith("@"):
        uname_lookup = target_arg.lstrip("@").lower()
        # DB mein dhundo
        doc = (await db.granted_users.find_one({"username": uname_lookup}) or
               await db.denied_users.find_one({"username": uname_lookup}))
        if doc and doc.get("user_id"):
            target_uid = doc["user_id"]
        # Telegram se live fetch
        try:
            chat = await bot.get_chat(f"@{uname_lookup}")
            target_uid = target_uid or chat.id
            tg_info = chat
        except: pass
        if not target_uid:
            return await message.answer(f"❌ @{uname_lookup} nahi mila.", parse_mode="Markdown")
    else:
        try: target_uid = int(target_arg)
        except: return await message.answer("❌ Valid User ID ya @username dein.", parse_mode="Markdown")
        try:
            tg_info = await bot.get_chat(target_uid)
        except: pass

    # ── Telegram live info ────────────────────────────────────
    tg_name     = tg_info.full_name if tg_info and hasattr(tg_info, "full_name") else None
    tg_username = tg_info.username  if tg_info else None

    # ── DB status check ───────────────────────────────────────
    granted_doc = await db.granted_users.find_one({"user_id": target_uid})
    denied_doc  = await db.denied_users.find_one({"user_id": target_uid})
    if granted_doc:
        status = "✅ Granted"
        if granted_doc.get("pending"): status = "⏳ Pending"
    elif denied_doc:
        status = "🚫 Denied"
    else:
        status = "👤 Unknown"

    # ── Albums ────────────────────────────────────────────────
    albums = await albums_col.find({"created_by": target_uid}).sort("created_at", -1).to_list(50)

    # ── Build response ────────────────────────────────────────
    text = "👤 *User Info*\n━━━━━━━━━━━━━━━━━━\n"
    if tg_name:    text += f"📛 {tg_name}\n"
    if tg_username: text += f"🔗 @{tg_username}\n"
    text += f"🆔 `{target_uid}`\n"
    text += f"📊 Status: {status}\n"

    if granted_doc:
        raw = granted_doc.get("granted_at", now_db())
        if raw.tzinfo is None:
            from datetime import timezone
            raw = raw.replace(tzinfo=timezone.utc)
        text += f"📅 Granted: {raw.astimezone(IST).strftime('%d %b %Y, %I:%M %p')} IST\n"
    elif denied_doc:
        raw = denied_doc.get("denied_at", now_db())
        if raw.tzinfo is None:
            from datetime import timezone
            raw = raw.replace(tzinfo=timezone.utc)
        text += f"📅 Denied: {raw.astimezone(IST).strftime('%d %b %Y, %I:%M %p')} IST\n"

    text += f"\n📁 *Albums ({len(albums)}):*\n"
    if albums:
        for alb in albums:
            alb_date = alb.get("created_at", now_db()).strftime("%d %b %Y, %I:%M %p")
            text += f"\n• **{alb['name']}**\n  🆔 `{alb['album_id']}` | 🗂 {alb['count']} files\n  📅 {alb_date}\n"
    else:
        text += "Koi album nahi banya.\n"

    await message.answer(text, parse_mode="Markdown")


# ============================================================
# /id
# ============================================================
@dp.message(Command("id"))
async def cmd_id(message: types.Message):
    user = message.from_user
    uname = f"@{user.username}" if user.username else "N/A"
    await message.answer(f"👤 **Your Info:**\n🆔 User ID: `{user.id}`\n📛 Name: {user.full_name}\n🔗 Username: {uname}", parse_mode="Markdown")


# ============================================================
# UNKNOWN COMMAND
# ============================================================
@dp.message(F.text.startswith("/"))
async def unknown_command(message: types.Message):
    if not is_admin(message.from_user.id): return
    await message.answer("YOU ARE NOT MY SENPAI 😤")


# ============================================================
# INLINE BUTTON CALLBACKS — do_zip_ / do_view_
# ============================================================
@dp.callback_query(F.data.startswith("do_zip_"))
async def cb_do_zip(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer("🚫 Access Denied!", show_alert=True)
    aid = callback.data.replace("do_zip_", "")
    await callback.answer("📦 ZIP shuru ho raha hai...")
    callback.message.text = f"/zip {aid}"
    await cmd_zip(callback.message)

@dp.callback_query(F.data.startswith("do_view_"))
async def cb_do_view(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer("🚫 Access Denied!", show_alert=True)
    aid = callback.data.replace("do_view_", "")
    await callback.answer("👁 Loading...")
    callback.message.text = f"/view_{aid}"
    await view_by_id(callback.message)


# ============================================================
# ERROR HANDLER
# ============================================================
@dp.error()
async def error_handler(event: types.ErrorEvent):
    logger.error(f"Error: {event.exception}", exc_info=True)


# ============================================================
# MAIN
# ============================================================
async def main():
    logger.info("🚀 Personal Cloud Bot starting...")
    try:
        await client.admin.command("ping")
        logger.info("✅ MongoDB connected!")
        await albums_col.create_index([("name", 1)])
        await albums_col.create_index([("album_id", 1)], unique=True, sparse=True)
        await albums_col.create_index([("tags", 1)])
        await albums_col.create_index([("created_by", 1)])
        await db.granted_users.create_index([("user_id", 1)])
        await db.granted_users.create_index([("username", 1)])
        await b2_history_col.create_index([("sent_at", -1)])
        await db.reg_codes.create_index([("user_id", 1)], unique=True)
        await db.reg_codes.create_index([("code", 1)], unique=True)
        await db.denied_users.create_index([("user_id", 1)], unique=True)
        granted_docs = await db.granted_users.find({"user_id": {"$ne": None}, "pending": {"$ne": True}}).to_list(500)
        for doc in granted_docs:
            if doc.get("user_id"): granted_users.add(doc["user_id"])
        logger.info(f"✅ {len(granted_users)} granted users loaded!")
        logger.info("✅ Bot polling started!")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"❌ Startup error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
