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
granted_users: set = set()


# ============================================================
# HELPERS
# ============================================================
def is_owner(uid): return uid == ADMIN_ID
def is_admin(uid): return uid == ADMIN_ID or uid in granted_users

async def find_album(identifier: str):
    return await albums_col.find_one({
        "$or": [
            {"name": {"$regex": f"^{re.escape(identifier)}$", "$options": "i"}},
            {"album_id": identifier}
        ]
    })

def count_media(files):
    photos = videos = docs = audios = 0
    for item in files:
        t = item.get("type", "photo") if isinstance(item, dict) else "photo"
        if t == "video": videos += 1
        elif t == "document": docs += 1
        elif t in ("audio", "voice"): audios += 1
        else: photos += 1
    return photos, videos, docs, audios


# ============================================================
# /start
# ============================================================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    uid = message.from_user.id
    username = (message.from_user.username or "").lower()

    if username:
        pending = await db.granted_users.find_one({"username": username, "pending": True})
        if pending:
            granted_users.add(uid)
            await db.granted_users.update_one(
                {"username": username},
                {"$set": {"user_id": uid, "pending": False}}
            )
            logger.info(f"✅ Pending grant activated: @{username} = {uid}")

    if not is_admin(uid):
        return await message.answer("🚫 Access Denied!")

    text = (
        "☁️ **Personal Cloud Bot**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📁 **Album Management**\n"
        "`/album <name>` — Naya album banayein\n"
        "`/add <name/id>` — Photos add karein\n"
        "`/close` — Album preview & save\n"
        "`/save_add` — Add session save karein\n"
        "`/cancel` — Session cancel karein\n\n"
        "🔧 **Organize**\n"
        "`/lock <name/id>` — Album lock karein\n"
        "`/unlock <name/id>` — Album unlock karein\n"
        "`/rename <old> <new>` — Album rename karein\n"
        "`/delete <name/id>` — Album delete karein\n"
        "`/merge <id1> <id2> <name>` — 2 albums combine karein\n"
        "`/duplicate <id> <name>` — Album copy karein\n"
        "`/tag <id> #tag1 #tag2` — Tags add karein\n"
        "`/dlt <name/id>` — Selective file delete\n\n"
        "🔍 **View & Search**\n"
        "`/albums` — Saare albums list karein\n"
        "`/search <name/id/#tag>` — Album search karein\n"
        "`/view_<id>` — Album files dekhein\n"
        "`/info <name/id>` — Album full details\n"
        "`/stats` — Cloud stats\n"
        "`/recent` — Last 5 updated albums\n\n"
        "📤 **Share**\n"
        "`/b2 <id> <@user/userid>` — Album kisi ko bhejein\n"
        "`/b2 <id> @u1 @u2 @u3` — Multiple logon ko bhejein\n"
        "`/b2list` — Share history dekhein\n"
        "`/zip <name/id>` — ZIP file banayein\n\n"
        "👥 **Access (Owner only)**\n"
        "`/grant <id/@user>` — Access dein\n"
        "`/denied <id/@user>` — Access hatayein\n"
        "`/grantlist` — Granted users list\n"
        "`/grantlistinfo <userid>` — User ki album history\n\n"
        "🆔 `/id` — Apna User ID dekhein"
    )
    await message.answer(text, parse_mode="Markdown")


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
        del user_sessions[message.from_user.id]

    user_sessions[message.from_user.id] = {
        "mode": "create", "name": name,
        "photos": [], "ids": set(), "started_at": now_ist()
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
async def _handle_media(message: types.Message, file_id: str, unique_id: str, media_type: str, fname: str = ""):
    uid = message.from_user.id
    if uid not in user_sessions:
        return

    session = user_sessions[uid]
    if unique_id in session["ids"]:
        return await message.reply(f"🚫 Duplicate {media_type}! Skip kar diya.")

    session["photos"].append({"file_id": file_id, "type": media_type, "name": fname})
    session["ids"].add(unique_id)
    count = len(session["photos"])

    if count == 1 or count % 5 == 0:
        kb = InlineKeyboardBuilder()
        if session["mode"] == "create":
            kb.button(text="✅ Close & Preview", callback_data="quick_close")
        else:
            kb.button(text="✅ Save Add", callback_data="quick_save_add")
        kb.button(text="❌ Cancel", callback_data="quick_cancel")
        await message.reply(
            f"✅ {media_type.capitalize()} #{count} add ho gaya!\n"
            f"Bhejte rahein ya neeche button dabayein.",
            reply_markup=kb.as_markup()
        )

@dp.message(F.photo)
async def handle_photo(message: types.Message):
    if message.from_user.id not in user_sessions: return
    p = message.photo[-1]
    await _handle_media(message, p.file_id, p.file_unique_id, "photo")

@dp.message(F.video)
async def handle_video(message: types.Message):
    if message.from_user.id not in user_sessions: return
    await _handle_media(message, message.video.file_id, message.video.file_unique_id, "video")

@dp.message(F.document)
async def handle_document(message: types.Message):
    if message.from_user.id not in user_sessions: return
    d = message.document
    await _handle_media(message, d.file_id, d.file_unique_id, "document", d.file_name or "")

@dp.message(F.audio)
async def handle_audio(message: types.Message):
    if message.from_user.id not in user_sessions: return
    await _handle_media(message, message.audio.file_id, message.audio.file_unique_id, "audio")

@dp.message(F.voice)
async def handle_voice(message: types.Message):
    if message.from_user.id not in user_sessions: return
    await _handle_media(message, message.voice.file_id, message.voice.file_unique_id, "voice")


# Quick action callbacks
@dp.callback_query(F.data == "quick_close")
async def quick_close(callback: types.CallbackQuery):
    await callback.answer()
    callback.message.text = "/close"
    callback.message.from_user = callback.from_user
    await cmd_close(callback.message)

@dp.callback_query(F.data == "quick_save_add")
async def quick_save_add_cb(callback: types.CallbackQuery):
    await callback.answer()
    callback.message.from_user = callback.from_user
    await save_add(callback.message)

@dp.callback_query(F.data == "quick_cancel")
async def quick_cancel_cb(callback: types.CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid in user_sessions:
        del user_sessions[uid]
    await callback.message.reply("❌ Session cancel ho gaya.")


# ============================================================
# /close - Preview & Save
# ============================================================
@dp.message(Command("close"))
async def cmd_close(message: types.Message):
    uid = message.from_user.id
    if uid not in user_sessions or user_sessions[uid]["mode"] != "create":
        return await message.answer("⚠️ Koi active album creation session nahi hai.")

    session = user_sessions[uid]
    if not session["photos"]:
        del user_sessions[uid]
        return await message.answer("⚠️ Koi file nahi thi. Session cancel ho gaya.")

    auto_id = f"ALB-{now_ist().strftime('%y%m%d%H%M')}"
    duration = (now_ist() - session["started_at"]).seconds // 60
    photos, videos, docs, audios = count_media(session["photos"])

    stats = ""
    if photos: stats += f"📸 {photos} photos\n"
    if videos: stats += f"🎥 {videos} videos\n"
    if docs: stats += f"📄 {docs} documents\n"
    if audios: stats += f"🎵 {audios} audio\n"

    preview_caption = (
        f"📝 **ALBUM PREVIEW**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📁 Name: **{session['name']}**\n"
        f"🆔 ID: `{auto_id}`\n"
        f"{stats}"
        f"⏱ Session: ~{duration} min\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Save karna chahte hain?"
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
    except TelegramBadRequest as e:
        logger.error(f"Preview error: {e}")
        await message.answer("❌ Preview generate nahi ho saka.")


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
        album_doc = {
            "album_id": album_id,
            "name": session["name"],
            "photos": session["photos"],
            "count": len(session["photos"]),
            "locked": False,
            "tags": [],
            "created_by": uid,
            "created_by_username": callback.from_user.username or "",
            "created_at": now_ist(),
            "updated_at": now_ist(),
            "history": [{
                "action": "created",
                "count": len(session["photos"]),
                "by": uid,
                "at": now_ist()
            }],
            "media_count": {"photos": photos, "videos": videos, "docs": docs, "audios": audios}
        }

        try:
            await albums_col.insert_one(album_doc)
            user = callback.from_user
            user_info = f"@{user.username}" if user.username else f"ID: {user.id}"

            await bot.send_message(STORAGE_CHANNEL,
                f"📁 **Album Created**\nName: {session['name']}\nCreated by: {user_info}",
                parse_mode="Markdown")

            for item in session["photos"]:
                fid = item["file_id"] if isinstance(item, dict) else item
                mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
                try:
                    if mtype == "video": await bot.send_video(STORAGE_CHANNEL, fid)
                    elif mtype == "document": await bot.send_document(STORAGE_CHANNEL, fid)
                    elif mtype == "audio": await bot.send_audio(STORAGE_CHANNEL, fid)
                    elif mtype == "voice": await bot.send_voice(STORAGE_CHANNEL, fid)
                    else: await bot.send_photo(STORAGE_CHANNEL, fid)
                    await asyncio.sleep(0.2)
                except Exception as ex:
                    logger.error(f"Channel send error: {ex}")

            stats_text = ""
            if photos: stats_text += f"📸 {photos} "
            if videos: stats_text += f"🎥 {videos} "
            if docs: stats_text += f"📄 {docs} "

            await bot.send_message(STORAGE_CHANNEL,
                f"✅ **Album Saved & Stored**\n"
                f"🆔 ID: `{album_id}`\n"
                f"📁 Name: {session['name']}\n"
                f"🗂 Files: {len(session['photos'])} ({stats_text.strip()})\n"
                f"🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
                parse_mode="Markdown")

            await callback.message.edit_caption(
                caption=f"✅ **Album Saved!**\n\n"
                        f"📁 **{session['name']}**\n"
                        f"🆔 `{album_id}`\n"
                        f"🗂 {len(session['photos'])} files\n"
                        f"👁 /view_{album_id}",
                parse_mode="Markdown"
            )
            await callback.answer("✅ Saved!")

        except Exception as e:
            logger.error(f"Save error: {e}")
            await callback.message.answer("❌ Save error. Retry karein.")
    else:
        await callback.answer("❌ Cancelled")
        await callback.message.edit_caption(caption="❌ Album save cancel.", parse_mode="Markdown")

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
        "started_at": now_ist()
    }

    await message.answer(
        f"➕ **Adding to: {album['name']}**\n"
        f"🆔 `{album['album_id']}` | Current: {album['count']} files\n\n"
        f"Files bhejein, phir `/save_add`\n❌ Cancel: `/cancel`",
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

        await albums_col.update_one(
            {"_id": session["db_id"]},
            {
                "$push": {
                    "photos": {"$each": session["photos"]},
                    "history": {
                        "action": "added",
                        "count": new_count,
                        "by": uid,
                        "at": now_ist()
                    }
                },
                "$inc": {
                    "count": new_count,
                    "media_count.photos": new_photos,
                    "media_count.videos": new_videos,
                    "media_count.docs": new_docs,
                    "media_count.audios": new_audios
                },
                "$set": {"updated_at": now_ist()}
            }
        )

        user = message.from_user
        user_info = f"@{user.username}" if user.username else f"ID: {user.id}"

        await bot.send_message(STORAGE_CHANNEL,
            f"📁 **Photos Added**\nName: {session['name']}\nBy: {user_info}",
            parse_mode="Markdown")

        for item in session["photos"]:
            fid = item["file_id"] if isinstance(item, dict) else item
            mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
            try:
                if mtype == "video": await bot.send_video(STORAGE_CHANNEL, fid)
                elif mtype == "document": await bot.send_document(STORAGE_CHANNEL, fid)
                elif mtype == "audio": await bot.send_audio(STORAGE_CHANNEL, fid)
                else: await bot.send_photo(STORAGE_CHANNEL, fid)
                await asyncio.sleep(0.2)
            except Exception as ex:
                logger.error(f"Channel add error: {ex}")

        await bot.send_message(STORAGE_CHANNEL,
            f"➕ **Photos Added**\n"
            f"📁 {session['name']} | 🆔 `{session['album_id']}`\n"
            f"🗂 +{new_count} files\n"
            f"🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
            parse_mode="Markdown")

        await message.answer(
            f"✅ **+{new_count} files** add ho gayi!\n📁 **{session['name']}**",
            parse_mode="Markdown"
        )
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
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"locked": True, "updated_at": now_ist()}})
    await message.answer(f"🔒 **'{album['name']}'** locked!", parse_mode="Markdown")

@dp.message(Command("unlock"))
async def cmd_unlock(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/unlock AlbumName`", parse_mode="Markdown")
    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"locked": False, "updated_at": now_ist()}})
    await message.answer(f"🔓 **'{album['name']}'** unlocked!", parse_mode="Markdown")


# ============================================================
# /rename
# ============================================================
@dp.message(Command("rename"))
async def cmd_rename(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    parts = message.text.split(maxsplit=1)
    text = parts[1].strip() if len(parts) > 1 else ""
    quoted = re.findall(r"['\"](.+?)['\"]", text)
    if len(quoted) >= 2:
        old_name, new_name = quoted[0].strip(), quoted[1].strip()
    else:
        simple = text.split()
        if len(simple) < 2:
            return await message.answer("❌ Usage: `/rename OldName NewName` ya `/rename 'Old Name' 'New Name'`", parse_mode="Markdown")
        old_name, new_name = simple[0], simple[1]

    album = await find_album(old_name)
    if not album: return await message.answer(f"❌ **'{old_name}'** nahi mila.", parse_mode="Markdown")
    conflict = await albums_col.find_one({"name": {"$regex": f"^{re.escape(new_name)}$", "$options": "i"}})
    if conflict: return await message.answer(f"⚠️ **'{new_name}'** already exists!", parse_mode="Markdown")
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"name": new_name, "updated_at": now_ist()}})
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
        f"⚠️ **Delete Confirmation**\n\n"
        f"📁 **{album['name']}**\n🆔 `{album['album_id']}`\n🗂 {album['count']} files\n\n"
        f"Yeh action **undo nahi** ho sakta!",
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
        await bot.send_message(STORAGE_CHANNEL,
            f"🗑️ **Album Deleted**\nID: `{album_id}`\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
            parse_mode="Markdown")
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

    # Store dlt session
    user_sessions[message.from_user.id] = {
        "mode": "dlt",
        "album_id": album["album_id"],
        "album_name": album["name"],
        "files": files,
        "selected": set()  # indices to delete
    }

    await message.answer(
        f"🗑️ **Selective Delete: {album['name']}**\n"
        f"🗂 {len(files)} files\n\n"
        f"Ab files bhej raha hoon — har ek ke niche ✅/❌ button hoga.\n"
        f"❌ wali files delete hongi.",
        parse_mode="Markdown"
    )

    # Send all files with toggle buttons
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
        except:
            pass
        await asyncio.sleep(0.3)

    # Bottom action buttons
    action_kb = InlineKeyboardBuilder()
    action_kb.row(
        types.InlineKeyboardButton(text="👁 Preview Deletions", callback_data=f"dlt_preview_{album['album_id']}"),
        types.InlineKeyboardButton(text="💾 Save Changes", callback_data=f"dlt_save_{album['album_id']}"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="dlt_cancel")
    )
    await message.answer(
        "⬆️ Files dekhein — **✅ Keep** pe click karein woh delete karne ke liye (❌ red ho jayega).\n"
        "Phir **Save Changes** dabayein.",
        reply_markup=action_kb.as_markup(), parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("dlt_toggle_"))
async def dlt_toggle(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid].get("mode") != "dlt":
        return await callback.answer("Session expire ho gaya.", show_alert=True)

    parts = callback.data.split("_")
    idx = int(parts[4])
    session = user_sessions[uid]

    if idx in session["selected"]:
        session["selected"].discard(idx)
        new_btn = "✅ Keep"
        new_cb = f"dlt_toggle_{session['album_id']}_{idx}_keep"
    else:
        session["selected"].add(idx)
        new_btn = "❌ Delete"
        new_cb = f"dlt_toggle_{session['album_id']}_{idx}_del"

    kb = InlineKeyboardBuilder()
    kb.button(text=new_btn, callback_data=new_cb)
    try:
        await callback.message.edit_reply_markup(reply_markup=kb.as_markup())
    except:
        pass
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
        f"👁 **Delete Preview**\n\n"
        f"❌ Delete hongi: {', '.join(map(str, del_nums))}\n"
        f"✅ Raheingi: {', '.join(map(str, keep_nums))}",
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
    album_name = session["album_name"]

    kb = InlineKeyboardBuilder()
    kb.row(
        types.InlineKeyboardButton(text="🗑️ Haan, Delete Karo", callback_data=f"dlt_confirm_{session['album_id']}"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="dlt_cancel")
    )
    await callback.answer()
    await callback.message.answer(
        f"⚠️ **Delete Confirmation**\n\n"
        f"📁 Album: **{album_name}**\n"
        f"❌ Delete: File {', '.join(map(str, del_nums))}\n"
        f"✅ Raheingi: File {', '.join(map(str, keep_nums))}\n\n"
        f"Kya aap sure hain? Yeh action **undo nahi** ho sakta!",
        reply_markup=kb.as_markup(), parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("dlt_confirm_"))
async def dlt_confirm(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid].get("mode") != "dlt":
        return await callback.answer("Session expire.", show_alert=True)
    session = user_sessions[uid]
    album_id = session["album_id"]
    files = session["files"]
    selected = session["selected"]

    new_files = [f for i, f in enumerate(files) if i not in selected]
    del_count = len(selected)

    photos, videos, docs, audios = count_media(new_files)
    await albums_col.update_one(
        {"album_id": album_id},
        {
            "$set": {
                "photos": new_files,
                "count": len(new_files),
                "updated_at": now_ist(),
                "media_count": {"photos": photos, "videos": videos, "docs": docs, "audios": audios}
            },
            "$push": {
                "history": {
                    "action": "deleted",
                    "count": -del_count,
                    "by": uid,
                    "at": now_ist()
                }
            }
        }
    )

    del user_sessions[uid]
    await callback.answer("🗑️ Done!")
    await callback.message.edit_text(
        f"✅ **{del_count} files delete ho gayi!**\n"
        f"📁 Album: **{session['album_name']}**\n"
        f"🗂 Remaining: {len(new_files)} files",
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
    args = message.text.split(maxsplit=3)
    if len(args) < 4:
        return await message.answer("❌ Usage: `/merge ALB-xxx ALB-yyy NewAlbumName`", parse_mode="Markdown")

    a1 = await find_album(args[1].strip())
    a2 = await find_album(args[2].strip())
    new_name = args[3].strip()

    if not a1: return await message.answer(f"❌ Album 1 '{args[1]}' nahi mila.", parse_mode="Markdown")
    if not a2: return await message.answer(f"❌ Album 2 '{args[2]}' nahi mila.", parse_mode="Markdown")

    conflict = await albums_col.find_one({"name": {"$regex": f"^{re.escape(new_name)}$", "$options": "i"}})
    if conflict: return await message.answer(f"⚠️ **'{new_name}'** already exists!", parse_mode="Markdown")

    merged_files = a1.get("photos", []) + a2.get("photos", [])
    photos, videos, docs, audios = count_media(merged_files)
    new_id = f"ALB-{now_ist().strftime('%y%m%d%H%M%S')}"

    await albums_col.insert_one({
        "album_id": new_id, "name": new_name,
        "photos": merged_files, "count": len(merged_files),
        "locked": False, "tags": [],
        "created_by": message.from_user.id,
        "created_at": now_ist(), "updated_at": now_ist(),
        "history": [{"action": "merged", "from": [a1["album_id"], a2["album_id"]], "by": message.from_user.id, "at": now_ist()}],
        "media_count": {"photos": photos, "videos": videos, "docs": docs, "audios": audios}
    })

    await message.answer(
        f"✅ **Albums Merged!**\n\n"
        f"📁 **{a1['name']}** ({a1['count']}) + **{a2['name']}** ({a2['count']})\n"
        f"➡️ **{new_name}** | 🆔 `{new_id}`\n"
        f"🗂 Total: {len(merged_files)} files",
        parse_mode="Markdown"
    )


# ============================================================
# /duplicate
# ============================================================
@dp.message(Command("duplicate"))
async def cmd_duplicate(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.answer("❌ Usage: `/duplicate ALB-xxx NewName`", parse_mode="Markdown")

    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")
    new_name = args[2].strip()
    conflict = await albums_col.find_one({"name": {"$regex": f"^{re.escape(new_name)}$", "$options": "i"}})
    if conflict: return await message.answer(f"⚠️ **'{new_name}'** already exists!", parse_mode="Markdown")

    new_id = f"ALB-{now_ist().strftime('%y%m%d%H%M%S')}"
    new_album = {**album, "_id": None, "album_id": new_id, "name": new_name,
                 "created_at": now_ist(), "updated_at": now_ist()}
    new_album.pop("_id")
    await albums_col.insert_one(new_album)
    await message.answer(
        f"✅ **Album Duplicated!**\n📁 **{album['name']}** → **{new_name}**\n🆔 `{new_id}`",
        parse_mode="Markdown"
    )


# ============================================================
# /tag
# ============================================================
@dp.message(Command("tag"))
async def cmd_tag(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.answer("❌ Usage: `/tag ALB-xxx #holi #family`", parse_mode="Markdown")

    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")

    new_tags = re.findall(r"#\w+", args[2])
    if not new_tags: return await message.answer("❌ Koi valid tag nahi mila. Use `#tagname`", parse_mode="Markdown")

    existing_tags = album.get("tags", [])
    all_tags = list(set(existing_tags + [t.lower() for t in new_tags]))
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"tags": all_tags, "updated_at": now_ist()}})
    await message.answer(
        f"🏷️ **Tags Updated!**\n📁 **{album['name']}**\nTags: {' '.join(all_tags)}",
        parse_mode="Markdown"
    )


# ============================================================
# /search
# ============================================================
@dp.message(Command("search"))
async def cmd_search(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/search query` ya `/search #tag`", parse_mode="Markdown")

    query = args[1].strip()

    if query.startswith("#"):
        cursor = albums_col.find({"tags": query.lower()}).sort("created_at", -1).limit(10)
    else:
        cursor = albums_col.find({
            "$or": [
                {"name": {"$regex": re.escape(query), "$options": "i"}},
                {"album_id": {"$regex": re.escape(query), "$options": "i"}}
            ]
        }).sort("created_at", -1).limit(10)

    results = await cursor.to_list(length=10)
    if not results:
        return await message.answer(f"🔍 **'{query}'** ke liye koi album nahi mila.", parse_mode="Markdown")

    response = f"🔍 **'{query}'** — {len(results)} mila\n\n"
    for alb in results:
        lock = "🔒" if alb.get("locked") else "🔓"
        date = alb.get("created_at", now_ist()).strftime("%d %b %Y")
        tags = " ".join(alb.get("tags", []))
        response += (
            f"{lock} **{alb['name']}**\n"
            f"🆔 `{alb['album_id']}` | 🗂 {alb['count']} files | 📅 {date}\n"
        )
        if tags: response += f"🏷️ {tags}\n"
        response += f"📦 /zip_{alb['album_id']} | 👁 /view_{alb['album_id']}\n\n"

    await message.answer(response, parse_mode="Markdown")


# ============================================================
# /albums
# ============================================================
@dp.message(Command("albums"))
async def cmd_list(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")

    try:
        albums = await albums_col.find().sort("created_at", -1).to_list(length=50)
        if not albums:
            return await message.answer("📂 Cloud empty hai! `/album <naam>` se banayein.", parse_mode="Markdown")

        total_photos = sum(a.get("count", 0) for a in albums)
        locked_count = sum(1 for a in albums if a.get("locked"))

        header = (
            f"☁️ **Personal Cloud**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📊 {len(albums)} albums | 🗂 {total_photos} files | 🔒 {locked_count} locked\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
        )

        lines = []
        for alb in albums:
            icon = "🔒" if alb.get("locked") else "📁"
            aid = alb.get("album_id") or "N/A"
            name = alb.get("name") or "Unnamed"
            count = alb.get("count", 0)
            date = alb.get("created_at", now_ist()).strftime("%d %b %Y, %I:%M %p")
            tags = " ".join(alb.get("tags", []))
            tag_line = f"\n   🏷️ {tags}" if tags else ""

            lines.append(
                f"{icon} **{name}**\n"
                f"   🆔 `{aid}` | 🗂 {count} files\n"
                f"   📅 {date}{tag_line}\n"
                f"   📦 /zip_{aid} | 👁 /view_{aid}"
            )

        body = "\n\n".join(lines)
        full_text = header + body

        if len(full_text) > 4000:
            await message.answer(header, parse_mode="Markdown")
            chunk = ""
            for line in lines:
                if len(chunk) + len(line) > 3800:
                    await message.answer(chunk, parse_mode="Markdown")
                    chunk = ""
                chunk += line + "\n\n"
            if chunk: await message.answer(chunk, parse_mode="Markdown")
        else:
            await message.answer(full_text, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"/albums error: {e}")
        await message.answer(f"❌ Error: `{e}`", parse_mode="Markdown")


# ============================================================
# /info
# ============================================================
@dp.message(Command("info"))
async def cmd_info(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/info AlbumName` ya `/info ALB-xxx`", parse_mode="Markdown")

    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")

    mc = album.get("media_count", {})
    photos = mc.get("photos", 0)
    videos = mc.get("videos", 0)
    docs = mc.get("docs", 0)
    audios = mc.get("audios", 0)

    # If media_count not set, count from files
    if not mc:
        photos, videos, docs, audios = count_media(album.get("photos", []))

    aid = album["album_id"]
    tags = " ".join(album.get("tags", [])) or "None"
    lock = "🔒 Locked" if album.get("locked") else "🔓 Unlocked"
    created = album.get("created_at", now_ist()).strftime("%d %b %Y, %I:%M %p")
    by_username = album.get("created_by_username", "")
    by_str = f"@{by_username}" if by_username else f"ID: {album.get('created_by', 'N/A')}"

    text = (
        f"📋 **Album Info**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📁 Name: **{album['name']}**\n"
        f"🆔 ID: `{aid}`\n"
        f"👁 View: /view_{aid}\n"
        f"👤 Created by: {by_str}\n"
        f"📅 Date: {created}\n"
        f"🔐 Status: {lock}\n\n"
        f"🗂 **Files:**\n"
    )
    if photos: text += f"   📸 Photos: {photos}\n"
    if videos: text += f"   🎥 Videos: {videos}\n"
    if docs: text += f"   📄 Documents: {docs}\n"
    if audios: text += f"   🎵 Audio: {audios}\n"
    text += f"   📊 Total: {album['count']}\n\n"

    # History
    history = album.get("history", [])
    if history:
        text += f"📜 **History:**\n"
        for h in history[-5:]:
            action = h.get("action", "")
            count = h.get("count", 0)
            at = h.get("at", now_ist())
            if isinstance(at, datetime): date_str = at.strftime("%d %b %Y")
            else: date_str = str(at)
            if action == "created": text += f"   ✨ Created | {date_str}\n"
            elif action == "added": text += f"   ➕ +{count} files | {date_str}\n"
            elif action == "deleted": text += f"   ➖ {count} files | {date_str}\n"
            elif action == "merged": text += f"   🔀 Merged | {date_str}\n"

    text += f"\n🏷️ Tags: {tags}\n"
    text += f"📦 ZIP: /zip_{aid}"

    await message.answer(text, parse_mode="Markdown")


# ============================================================
# /view_
# ============================================================
@dp.message(F.text.regexp(r"^/view_[A-Za-z0-9\-]+$"))
async def view_by_id(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    aid = message.text.replace("/view_", "").strip()
    album = await find_album(aid)
    if not album: return await message.answer(f"❌ Album `{aid}` nahi mila.", parse_mode="Markdown")

    await message.answer(
        f"📂 **{album['name']}**\n🆔 `{album['album_id']}` | 🗂 {album['count']} files\n_Loading..._",
        parse_mode="Markdown"
    )

    files = album.get("photos", [])
    sent = failed = 0
    for item in files:
        fid = item["file_id"] if isinstance(item, dict) else item
        mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
        try:
            if mtype == "video": await bot.send_video(message.chat.id, fid)
            elif mtype == "document": await bot.send_document(message.chat.id, fid)
            elif mtype == "audio": await bot.send_audio(message.chat.id, fid)
            else: await bot.send_photo(message.chat.id, fid)
            sent += 1
        except: failed += 1
        await asyncio.sleep(0.3)

    summary = f"✅ **{sent}/{len(files)} files** sent!"
    if failed: summary += f"\n⚠️ {failed} failed (expired IDs)."
    await message.answer(summary, parse_mode="Markdown")


# ============================================================
# /zip_ shortcut & /zip command
# ============================================================
@dp.message(F.text.regexp(r"^/zip_[A-Za-z0-9\-]+$"))
async def zip_shortcut(message: types.Message):
    aid = message.text.replace("/zip_", "").strip()
    message.text = f"/zip {aid}"
    await cmd_zip(message)

@dp.message(Command("zip"))
async def cmd_zip(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/zip AlbumName` ya `/zip ALB-xxx`", parse_mode="Markdown")

    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")

    files = album.get("photos", [])
    if not files: return await message.answer("❌ Album empty hai.", parse_mode="Markdown")

    status_msg = await message.answer(
        f"⏳ ZIP ban raha hai...\n📁 **{album['name']}** | 🗂 {len(files)} files",
        parse_mode="Markdown"
    )

    zippable = []
    videos = []
    for item in files:
        fid = item["file_id"] if isinstance(item, dict) else item
        mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
        fname = item.get("name", "") if isinstance(item, dict) else ""
        if mtype == "video": videos.append(fid)
        else: zippable.append((fid, mtype, fname))

    zip_buffer = io.BytesIO()
    zipped = failed = 0

    try:
        with zipfile.ZipFile(zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            for idx, (fid, mtype, fname) in enumerate(zippable, 1):
                try:
                    tg_file = await bot.get_file(fid)
                    file_url = f"https://api.telegram.org/file/bot{API_TOKEN}/{tg_file.file_path}"
                    async with aiohttp.ClientSession() as sess:
                        async with sess.get(file_url) as resp:
                            if resp.status == 200:
                                data = await resp.read()
                                ext = tg_file.file_path.split(".")[-1] if "." in tg_file.file_path else mtype
                                filename = fname if fname else f"{idx:03d}_{mtype}.{ext}"
                                zf.writestr(filename, data)
                                zipped += 1
                            else: failed += 1
                except Exception as e:
                    logger.error(f"ZIP error: {e}")
                    failed += 1

        zip_buffer.seek(0)

        if zipped > 0:
            zip_name = album['name']
            zip_msg = await bot.send_document(
                message.chat.id,
                document=types.BufferedInputFile(zip_buffer.read(), filename=f"{zip_name}.zip"),
                caption=f"📦 **{zip_name}.zip**\n🗂 {zipped} files\n⚠️ _Auto-delete: 5 min_",
                parse_mode="Markdown"
            )

            btn = InlineKeyboardBuilder()
            btn.row(
                types.InlineKeyboardButton(text="🗑️ Delete ZIP", callback_data=f"delzip_{zip_msg.message_id}_{message.chat.id}"),
                types.InlineKeyboardButton(text="📤 Share", switch_inline_query=f"")
            )
            await bot.edit_message_reply_markup(
                chat_id=message.chat.id, message_id=zip_msg.message_id,
                reply_markup=btn.as_markup()
            )

            async def auto_delete(mid=zip_msg.message_id, cid=message.chat.id, name=zip_name):
                await asyncio.sleep(300)
                try:
                    await bot.delete_message(cid, mid)
                    await bot.send_message(cid, f"🗑️ ZIP auto-deleted: **{name}.zip**", parse_mode="Markdown")
                except: pass
            asyncio.create_task(auto_delete())

        if videos:
            await message.answer(f"🎥 **{len(videos)} video(s)** alag se aa rahi hain...", parse_mode="Markdown")
            for fid in videos:
                try:
                    await bot.send_video(message.chat.id, fid)
                    await asyncio.sleep(0.3)
                except: pass

        summary = f"✅ ZIP ready!\n📦 {zipped} files zipped"
        if failed: summary += f"\n⚠️ {failed} failed"
        if videos: summary += f"\n🎥 {len(videos)} videos alag bheji"
        await status_msg.edit_text(summary, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"ZIP error: {e}")
        await status_msg.edit_text(f"❌ ZIP error: `{e}`", parse_mode="Markdown")

@dp.callback_query(F.data.startswith("delzip_"))
async def delete_zip_cb(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return await callback.answer("🚫", show_alert=True)
    try:
        parts = callback.data.split("_")
        await bot.delete_message(int(parts[2]), int(parts[1]))
        await callback.answer("🗑️ ZIP deleted!")
    except: await callback.answer("❌ Delete failed.", show_alert=True)


# ============================================================
# /recent
# ============================================================
@dp.message(Command("recent"))
async def cmd_recent(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    albums = await albums_col.find().sort("updated_at", -1).limit(5).to_list(5)
    if not albums: return await message.answer("📂 Koi album nahi.", parse_mode="Markdown")

    text = "🕐 **Recently Updated Albums:**\n\n"
    for alb in albums:
        date = alb.get("updated_at", now_ist()).strftime("%d %b %Y, %I:%M %p")
        text += f"📁 **{alb['name']}** | 🗂 {alb['count']} files\n📅 {date}\n👁 /view_{alb['album_id']}\n\n"
    await message.answer(text, parse_mode="Markdown")


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
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🟢 Bot: Online\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
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
        return await message.answer(
            "❌ Usage:\n`/b2 ALB-xxx @user`\n`/b2 ALB-xxx @u1 @u2 @u3`\n`/b2 AlbumName 123456789`",
            parse_mode="Markdown"
        )

    parts = args[1].split()
    if len(parts) < 2: return await message.answer("❌ Album ID aur recipient dein.", parse_mode="Markdown")

    album_id = parts[0]
    targets_raw = parts[1:]

    album = await find_album(album_id)
    if not album: return await message.answer(f"❌ Album '{album_id}' nahi mila.", parse_mode="Markdown")

    files = album.get("photos", [])
    if not files: return await message.answer("❌ Album empty hai.", parse_mode="Markdown")

    # Resolve all targets
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

    await message.answer(
        f"📤 Sending **{album['name']}** to {len(target_ids)} user(s)...",
        parse_mode="Markdown"
    )

    for uid, uname in target_ids:
        try:
            await bot.send_message(uid,
                f"📂 **{album['name']}**\n🗂 {len(files)} files\n_Loading..._",
                parse_mode="Markdown")
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

            # Log to history
            await b2_history_col.insert_one({
                "album_id": album["album_id"],
                "album_name": album["name"],
                "sent_by": message.from_user.id,
                "sent_to": uid,
                "sent_to_name": uname,
                "files_count": sent,
                "sent_at": now_ist()
            })
            await message.answer(f"✅ **{uname}** ko {sent} files bhej di!", parse_mode="Markdown")

        except Exception as e:
            await message.answer(f"❌ **{uname}** ko bhejne mein error: {e}", parse_mode="Markdown")


# ============================================================
# /b2list
# ============================================================
@dp.message(Command("b2list"))
async def cmd_b2list(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    history = await b2_history_col.find().sort("sent_at", -1).limit(20).to_list(20)
    if not history: return await message.answer("📋 Koi share history nahi.", parse_mode="Markdown")

    text = "📤 **Share History (Last 20):**\n\n"
    for h in history:
        date = h.get("sent_at", now_ist()).strftime("%d %b %Y, %I:%M %p")
        text += (
            f"📁 **{h.get('album_name', 'N/A')}**\n"
            f"➡️ To: {h.get('sent_to_name', h.get('sent_to'))}\n"
            f"🗂 {h.get('files_count', 0)} files | 📅 {date}\n\n"
        )
    await message.answer(text, parse_mode="Markdown")


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
            f"❌ **Session Cancel!**\nMode: {session.get('mode')} | Album: {session.get('name', '')}\n"
            f"_{len(session.get('photos', []))} unsaved files discard ho gayi._",
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
        now = now_ist()
        await bot.send_message(user_id,
            f"👋 **HEY {name}!**\n\n"
            f"🎉 **Grant Access Successfully!**\n\n"
            f"🥳 **ENJOY!!**\n\n"
            f"📅 **Access Date:** {now.strftime('%d %B %Y')}\n"
            f"🕐 **Access Time:** {now.strftime('%I:%M %p')} IST",
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
        return await message.answer(
            "❌ Usage:\n`/grant 123456789`\n`/grant @username`",
            parse_mode="Markdown"
        )

    target = args[1].strip()

    if target.lstrip("-").isdigit():
        uid = int(target)
        if uid == ADMIN_ID: return await message.answer("⚠️ Aap owner hain already!")
        granted_users.add(uid)
        await db.granted_users.update_one(
            {"user_id": uid},
            {"$set": {"user_id": uid, "username": None, "granted_at": now_ist(), "granted_by": message.from_user.id}},
            upsert=True
        )
        await message.answer(f"✅ **Access Granted!**\n🆔 `{uid}`", parse_mode="Markdown")
        ok = await send_greeting(uid)
        if not ok:
            await message.answer("⚠️ User ko greeting nahi gayi — user ne pehle /start kiya ho.", parse_mode="Markdown")

    elif target.startswith("@"):
        username = target.lstrip("@").lower()
        doc = await db.granted_users.find_one({"username": username})
        if doc and doc.get("user_id"):
            uid = doc["user_id"]
            granted_users.add(uid)
            await db.granted_users.update_one(
                {"user_id": uid},
                {"$set": {"granted_at": now_ist(), "granted_by": message.from_user.id}},
                upsert=True
            )
            await message.answer(f"✅ **Access Granted!**\n👤 @{username} | 🆔 `{uid}`", parse_mode="Markdown")
            ok = await send_greeting(uid, username)
            if not ok:
                await message.answer("⚠️ User ko greeting nahi gayi.", parse_mode="Markdown")
        else:
            await db.granted_users.update_one(
                {"username": username},
                {"$set": {"username": username, "user_id": None, "granted_at": now_ist(), "granted_by": message.from_user.id, "pending": True}},
                upsert=True
            )
            await message.answer(
                f"⏳ **Pending Grant!**\n👤 @{username}\nJab pehli baar /start karenge, activate ho jayega.\n💡 User ID zyada reliable hai.",
                parse_mode="Markdown"
            )
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
        r = await db.granted_users.delete_one({"user_id": uid})
        if r.deleted_count: await message.answer(f"🚫 Access removed!\n🆔 `{uid}`", parse_mode="Markdown")
        else: await message.answer(f"⚠️ `{uid}` list mein nahi tha.", parse_mode="Markdown")
    elif target.startswith("@"):
        username = target.lstrip("@").lower()
        doc = await db.granted_users.find_one({"username": username})
        if doc:
            if doc.get("user_id"): granted_users.discard(doc["user_id"])
            await db.granted_users.delete_one({"username": username})
            await message.answer(f"🚫 @{username} access removed!", parse_mode="Markdown")
        else: await message.answer(f"⚠️ @{username} list mein nahi tha.", parse_mode="Markdown")
    else: await message.answer("❌ Valid ID ya @username dein.", parse_mode="Markdown")


@dp.message(Command("grantlist"))
async def cmd_grantlist(message: types.Message):
    if not is_owner(message.from_user.id): return await message.answer("🚫 Sirf owner!")
    users = await db.granted_users.find().to_list(100)
    if not users: return await message.answer("📋 Koi granted user nahi.", parse_mode="Markdown")

    text = "👥 **Granted Users:**\n━━━━━━━━━━━━━━━━━━\n\n"
    for u in users:
        uid = u.get("user_id")
        uname = u.get("username")
        pending = u.get("pending", False)
        date = u.get("granted_at", now_ist()).strftime("%d %b %Y")
        status = "⏳ Pending" if pending else "✅ Active"
        id_str = f"`{uid}`" if uid else "-"
        name_str = f"@{uname}" if uname else "-"
        text += f"{status} | {name_str} | {id_str}\n📅 {date}\n\n"

    text += f"━━━━━━━━━━━━━━━━━━\nTotal: {len(users)}"
    await message.answer(text, parse_mode="Markdown")


@dp.message(Command("grantlistinfo"))
async def cmd_grantlistinfo(message: types.Message):
    if not is_owner(message.from_user.id): return await message.answer("🚫 Sirf owner!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/grantlistinfo userid`", parse_mode="Markdown")

    try: target_uid = int(args[1].strip())
    except: return await message.answer("❌ Valid User ID dein.", parse_mode="Markdown")

    albums = await albums_col.find({"created_by": target_uid}).sort("created_at", -1).to_list(50)
    user_doc = await db.granted_users.find_one({"user_id": target_uid})
    uname = f"@{user_doc['username']}" if user_doc and user_doc.get("username") else f"ID: {target_uid}"

    if not albums:
        return await message.answer(f"📋 **{uname}** ne koi album create nahi kiya.", parse_mode="Markdown")

    text = f"👤 **{uname}** ki Albums:\n━━━━━━━━━━━━━━━━━━\n\n"
    for alb in albums:
        date = alb.get("created_at", now_ist()).strftime("%d %b %Y, %I:%M %p")
        text += (
            f"📁 **{alb['name']}**\n"
            f"🆔 `{alb['album_id']}` | 🗂 {alb['count']} files\n"
            f"📅 {date}\n"
            f"👁 /view_{alb['album_id']}\n\n"
        )
    text += f"Total: {len(albums)} albums"
    await message.answer(text, parse_mode="Markdown")


# ============================================================
# /id
# ============================================================
@dp.message(Command("id"))
async def cmd_id(message: types.Message):
    user = message.from_user
    uname = f"@{user.username}" if user.username else "N/A"
    await message.answer(
        f"👤 **Your Info:**\n"
        f"🆔 User ID: `{user.id}`\n"
        f"📛 Name: {user.full_name}\n"
        f"🔗 Username: {uname}",
        parse_mode="Markdown"
    )


# ============================================================
# UNKNOWN COMMAND
# ============================================================
@dp.message(F.text.startswith("/"))
async def unknown_command(message: types.Message):
    if not is_admin(message.from_user.id): return
    await message.answer("YOU ARE NOT MY SENPAI 😤")


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

        granted_docs = await db.granted_users.find(
            {"user_id": {"$ne": None}, "pending": {"$ne": True}}
        ).to_list(500)
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
