import asyncio
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from motor.motor_asyncio import AsyncIOMotorClient
from aiogram.exceptions import TelegramBadRequest

# ============================================================
# 10) CONFIGURATION & SECURITY
# ============================================================
import os
from zoneinfo import ZoneInfo

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

# In-memory sessions for album creation/editing
user_sessions = {}


# ============================================================
# SECURITY HELPER
# ============================================================
def is_owner(user_id: int) -> bool:
    """Sirf bot ka original owner - ADMIN_ID"""
    return user_id == ADMIN_ID

def is_admin(user_id: int) -> bool:
    """Owner + granted users dono allowed"""
    return user_id == ADMIN_ID or user_id in granted_users

# Granted users ka in-memory + DB backed set
granted_users: set = set()

async def find_album(identifier: str):
    """Album ko naam ya ID dono se dhundho"""
    return await albums_col.find_one({
        "$or": [
            {"name": {"$regex": f"^{identifier}$", "$options": "i"}},
            {"album_id": identifier}
        ]
    })


# ============================================================
# /start - Welcome Message
# ============================================================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    uid = message.from_user.id
    username = (message.from_user.username or "").lower()

    # Pending username grant check - jab pehli baar message kare
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
        return await message.answer("🚫 Access Denied! Yeh bot sirf admin ke liye hai.")

    text = (
        "☁️ **Personal Cloud Bot - Active!**\n\n"
        "📋 **Available Commands:**\n\n"
        "**Album Management:**\n"
        "`/album <name>` - Naya album banayein\n"
        "`/add <name>` - Existing album mein photos add karein\n"
        "`/close` - Album finalize karein (preview + save)\n"
        "`/save_add` - Add session save karein\n\n"
        "**Organize:**\n"
        "`/lock <name>` - Album lock karein\n"
        "`/unlock <name>` - Album unlock karein\n"
        "`/rename <purana> <naya>` - Album rename karein\n"
        "`/delete <name>` - Album delete karein\n\n"
        "**View & Search:**\n"
        "`/albums` - Saare albums list karein\n"
        "`/search <query>` - Album search karein\n"
        "`/view_<album_id>` - Album photos dekhein\n"
        "`/stats` - Cloud stats dekhein\n"
        "`/cancel` - Current session cancel karein\n\n"
        "**Access Management (Owner only):**\n"
        "`/grant <id/@user>` - Kisi ko bot access dein\n"
        "`/denied <id/@user>` - Access wapis lo\n"
        "`/grantlist` - Saare granted users dekhein"
    )
    await message.answer(text, parse_mode="Markdown")


# ============================================================
# 1) ALBUM CREATION SYSTEM
# ============================================================
@dp.message(Command("album"))
async def cmd_album(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/album Trip2024`", parse_mode="Markdown")

    name = args[1].strip()

    # Check duplicate album name (case-insensitive)
    existing = await albums_col.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}})
    if existing:
        return await message.answer(
            f"⚠️ Album **'{name}'** pehle se exist karta hai!\n"
            f"ID: `{existing['album_id']}` | Photos: {existing['count']}\n"
            f"Koi aur naam chunein ya `/add {name}` se photos add karein.",
            parse_mode="Markdown"
        )

    # Cancel any existing session
    if message.from_user.id in user_sessions:
        del user_sessions[message.from_user.id]

    user_sessions[message.from_user.id] = {
        "mode": "create",
        "name": name,
        "photos": [],
        "ids": set(),
        "started_at": now_ist()
    }

    await message.answer(
        f"📸 **Album Creation Started!**\n\n"
        f"📁 Name: **{name}**\n"
        f"📤 Ab photos bhejiye...\n"
        f"✅ Khatam ho jaye to `/close` likhein\n"
        f"❌ Cancel karne ke liye `/cancel` likhein",
        parse_mode="Markdown"
    )


# ============================================================
# MEDIA HANDLER (photo, video, document, audio)
# ============================================================
async def _handle_media(message: types.Message, file_id: str, unique_id: str, media_type: str):
    uid = message.from_user.id
    if uid not in user_sessions:
        return

    session = user_sessions[uid]

    if unique_id in session["ids"]:
        return await message.reply(f"🚫 Duplicate {media_type}! Skip kar diya gaya.")

    session["photos"].append({"file_id": file_id, "type": media_type})
    session["ids"].add(unique_id)

    count = len(session["photos"])
    if count == 1 or count % 5 == 0:
        await message.reply(
            f"✅ {media_type.capitalize()} #{count} add ho gaya!\n"
            f"Bas bhejte rahein... /close ya /save_add se finish karein."
        )

@dp.message(F.photo)
async def handle_photo(message: types.Message):
    photo = message.photo[-1]
    await _handle_media(message, photo.file_id, photo.file_unique_id, "photo")

@dp.message(F.video)
async def handle_video(message: types.Message):
    await _handle_media(message, message.video.file_id, message.video.file_unique_id, "video")

@dp.message(F.document)
async def handle_document(message: types.Message):
    doc = message.document
    # Allow pdf, jpg, png, and other docs
    await _handle_media(message, doc.file_id, doc.file_unique_id, "document")

@dp.message(F.audio)
async def handle_audio(message: types.Message):
    await _handle_media(message, message.audio.file_id, message.audio.file_unique_id, "audio")

@dp.message(F.voice)
async def handle_voice(message: types.Message):
    await _handle_media(message, message.voice.file_id, message.voice.file_unique_id, "voice")


# ============================================================
# 2) PREVIEW & SAVE SYSTEM - /close
# ============================================================
@dp.message(Command("close"))
async def cmd_close(message: types.Message):
    uid = message.from_user.id
    if uid not in user_sessions or user_sessions[uid]["mode"] != "create":
        return await message.answer("⚠️ Koi active album creation session nahi hai.")

    session = user_sessions[uid]

    if not session["photos"]:
        del user_sessions[uid]
        return await message.answer("⚠️ Album mein koi photo nahi thi. Session cancel ho gaya.")

    auto_id = f"ALB-{now_ist().strftime('%y%m%d%H%M')}"
    duration = (now_ist() - session["started_at"]).seconds // 60

    preview_caption = (
        f"📝 **ALBUM PREVIEW**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📁 Name: **{session['name']}**\n"
        f"🖼 Photos: **{len(session['photos'])}**\n"
        f"🆔 Auto ID: `{auto_id}`\n"
        f"⏱ Session: ~{duration} min\n"
        f"🔒 Status: 🔓 Unlocked\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Save karna chahte hain?"
    )

    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="✅ Save Album", callback_data="confirm_save"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="confirm_cancel")
    )

    try:
        first_item = session["photos"][0]
        if isinstance(first_item, dict):
            fid = first_item["file_id"]
            mtype = first_item["type"]
        else:
            fid = first_item
            mtype = "photo"

        if mtype == "video":
            await bot.send_video(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        elif mtype == "document":
            await bot.send_document(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        else:
            await bot.send_photo(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
    except TelegramBadRequest as e:
        logger.error(f"Preview send failed: {e}")
        await message.answer("❌ Preview generate nahi ho saka. Dobara try karein.")


# ============================================================
# CALLBACKS - Save / Cancel
# ============================================================
@dp.callback_query(F.data.in_({"confirm_save", "confirm_cancel"}))
async def process_confirm(callback: types.CallbackQuery):
    uid = callback.from_user.id

    if uid not in user_sessions:
        await callback.answer("Session expire ho gaya!", show_alert=True)
        try:
            await callback.message.delete()
        except:
            pass
        return

    session = user_sessions[uid]

    if callback.data == "confirm_save":
        album_id = f"ALB-{now_ist().strftime('%y%m%d%H%M%S')}"
        album_doc = {
            "album_id": album_id,
            "name": session["name"],
            "photos": session["photos"],
            "count": len(session["photos"]),
            "locked": False,
            "created_at": now_ist(),
            "updated_at": now_ist()
        }

        try:
            await albums_col.insert_one(album_doc)

            # 9) Backup to Storage Channel
            user = callback.from_user
            user_info = f"@{user.username}" if user.username else f"ID: {user.id}"

            # Step 1: Album creation info message
            await bot.send_message(
                STORAGE_CHANNEL,
                f"📁 **Album Created**\n"
                f"Name: {session['name']}\n"
                f"Created by: {user_info}",
                parse_mode="Markdown"
            )

            # Step 2: Send all media to channel, track message IDs
            photos = session['photos']
            first_msg_id = None
            last_msg_id = None
            for i in range(0, len(photos), 10):
                batch = photos[i:i+10]
                for item in batch:
                    if isinstance(item, dict):
                        fid, mtype = item["file_id"], item["type"]
                    else:
                        fid, mtype = item, "photo"
                    try:
                        if mtype == "video":
                            sent = await bot.send_video(STORAGE_CHANNEL, fid)
                        elif mtype == "document":
                            sent = await bot.send_document(STORAGE_CHANNEL, fid)
                        elif mtype == "audio":
                            sent = await bot.send_audio(STORAGE_CHANNEL, fid)
                        elif mtype == "voice":
                            sent = await bot.send_voice(STORAGE_CHANNEL, fid)
                        else:
                            sent = await bot.send_photo(STORAGE_CHANNEL, fid)
                        if first_msg_id is None:
                            first_msg_id = sent.message_id
                        last_msg_id = sent.message_id
                    except Exception as ex:
                        logger.error(f"Channel media send error: {ex}")
                    await asyncio.sleep(0.2)

            # Step 3: Summary message with actual chat IDs
            await bot.send_message(
                STORAGE_CHANNEL,
                f"✅ **Album Saved & Stored**\n"
                f"🆔 ID: `{album_id}`\n"
                f"📁 Name: {session['name']}\n"
                f"🖼 Files: {len(photos)}\n"
                f"🕐 Time: {now_ist().strftime('%Y-%m-%d %H:%M')} IST",
                parse_mode="Markdown"
            )


            await callback.message.edit_caption(
                caption=f"✅ **Album Saved Successfully!**\n\n"
                        f"📁 Name: **{session['name']}**\n"
                        f"🆔 ID: `{album_id}`\n"
                        f"🖼 Files: {len(session['photos'])}\n"
                        f"📂 `/view_{album_id}` se dekh sakte hain",
                parse_mode="Markdown"
            )
            await callback.answer("✅ Album saved!")

        except Exception as e:
            logger.error(f"Album save error: {e}")
            await callback.message.answer("❌ Save karte waqt error aaya. Dobara try karein.")

    else:
        await callback.answer("❌ Cancelled")
        await callback.message.edit_caption(caption="❌ **Album save cancel kar diya gaya.**", parse_mode="Markdown")

    del user_sessions[uid]


# ============================================================
# 3) ADD TO EXISTING ALBUM - /add
# ============================================================
@dp.message(Command("add"))
async def cmd_add(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/add AlbumName`", parse_mode="Markdown")

    name = args[1].strip()
    # Search by name OR album_id
    album = await albums_col.find_one({
        "$or": [
            {"name": {"$regex": f"^{name}$", "$options": "i"}},
            {"album_id": name}
        ]
    })

    if not album:
        return await message.answer(
            f"❌ **'{name}'** naam ya ID ka album nahi mila.\n"
            f"Check ke liye `/albums` dekhein.",
            parse_mode="Markdown"
        )

    # 4) Lock check
    if album.get("locked"):
        return await message.answer(
            f"🔒 **'{name}'** locked hai!\n"
            f"Pehle `/unlock {name}` karein.",
            parse_mode="Markdown"
        )

    # Cancel any existing session
    if message.from_user.id in user_sessions:
        del user_sessions[message.from_user.id]

    user_sessions[message.from_user.id] = {
        "mode": "add",
        "db_id": album["_id"],
        "album_id": album["album_id"],
        "name": album["name"],
        "photos": [],
        "ids": set(album.get("photo_unique_ids", [])),  # Existing unique IDs for dup check
        "started_at": now_ist()
    }

    await message.answer(
        f"➕ **Adding to Album: {album['name']}**\n\n"
        f"🆔 ID: `{album['album_id']}`\n"
        f"🖼 Current Photos: {album['count']}\n\n"
        f"Photos bhejein, phir `/save_add` likhein.\n"
        f"❌ Cancel: `/cancel`",
        parse_mode="Markdown"
    )


# ============================================================
# 3) SAVE_ADD - Finalize adding photos
# ============================================================
@dp.message(Command("save_add"))
async def save_add(message: types.Message):
    uid = message.from_user.id
    if uid not in user_sessions or user_sessions[uid]["mode"] != "add":
        return await message.answer("⚠️ Koi active add session nahi hai.")

    session = user_sessions[uid]

    if not session["photos"]:
        del user_sessions[uid]
        return await message.answer("⚠️ Koi nai photo nahi bheji gayi. Session cancel.")

    try:
        await albums_col.update_one(
            {"_id": session["db_id"]},
            {
                "$push": {"photos": {"$each": session["photos"]}},
                "$inc": {"count": len(session["photos"])},
                "$set": {"updated_at": now_ist()}
            }
        )

        # 9) Backup log - send to channel with photos
        user = message.from_user
        user_info = f"@{user.username}" if user.username else f"ID: {user.id}"

        # Get current album to know existing photo count
        current_album = await albums_col.find_one({"_id": session["db_id"]})
        existing_count = (current_album.get("count", 0) - len(session["photos"])) if current_album else 0
        start_num = existing_count + 1
        end_num = existing_count + len(session["photos"])

        # Step 1: Photo added info message
        await bot.send_message(
            STORAGE_CHANNEL,
            f"📁 **Photos Added**\n"
            f"Name: {session['name']}\n"
            f"Created by: {user_info}",
            parse_mode="Markdown"
        )

        # Step 2: Send only new media to channel, track message IDs
        new_photos = session['photos']
        first_msg_id = None
        last_msg_id = None
        for i in range(0, len(new_photos), 10):
            batch = new_photos[i:i+10]
            media_group = []
            for item in batch:
                if isinstance(item, dict):
                    fid, mtype = item["file_id"], item["type"]
                else:
                    fid, mtype = item, "photo"
                try:
                    if mtype == "video":
                        sent = await bot.send_video(STORAGE_CHANNEL, fid)
                    elif mtype == "document":
                        sent = await bot.send_document(STORAGE_CHANNEL, fid)
                    elif mtype == "audio":
                        sent = await bot.send_audio(STORAGE_CHANNEL, fid)
                    elif mtype == "voice":
                        sent = await bot.send_voice(STORAGE_CHANNEL, fid)
                    else:
                        sent = await bot.send_photo(STORAGE_CHANNEL, fid)
                    if first_msg_id is None:
                        first_msg_id = sent.message_id
                    last_msg_id = sent.message_id
                except Exception as ex:
                    logger.error(f"Channel add media error: {ex}")
                await asyncio.sleep(0.2)

        # Step 3: Summary with actual chat IDs
        await bot.send_message(
            STORAGE_CHANNEL,
            f"➕ **Photos Added**\n"
            f"📁 Album: {session['name']}\n"
            f"🆔 ID: `{session['album_id']}`\n"
            f"🖼 Added: {len(session['photos'])} files\n"
            f"🕐 {now_ist().strftime('%Y-%m-%d %H:%M')} IST",
            parse_mode="Markdown"
        )


        await message.answer(
            f"✅ **{len(session['photos'])} photos** add ho gayi hain!\n"
            f"📁 Album: **{session['name']}**",
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"save_add error: {e}")
        await message.answer("❌ Photos save nahi ho sakin. Dobara try karein.")

    del user_sessions[uid]


# ============================================================
# 4) LOCK / UNLOCK SYSTEM
# ============================================================
@dp.message(Command("lock"))
async def cmd_lock(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/lock AlbumName`", parse_mode="Markdown")

    name = args[1].strip()
    album = await find_album(name)
    if not album:
        return await message.answer(f"❌ Album **'{name}'** nahi mila.", parse_mode="Markdown")

    await albums_col.update_one(
        {"_id": album["_id"]},
        {"$set": {"locked": True, "updated_at": now_ist()}}
    )
    await message.answer(f"🔒 Album **'{album['name']}'** lock ho gaya!\nAb koi file add nahi ki ja sakti.", parse_mode="Markdown")


@dp.message(Command("unlock"))
async def cmd_unlock(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/unlock AlbumName`", parse_mode="Markdown")

    name = args[1].strip()
    album = await find_album(name)
    if not album:
        return await message.answer(f"❌ Album **'{name}'** nahi mila.", parse_mode="Markdown")

    await albums_col.update_one(
        {"_id": album["_id"]},
        {"$set": {"locked": False, "updated_at": now_ist()}}
    )
    await message.answer(f"🔓 Album **'{album['name']}'** unlock ho gaya!\nAb files add ki ja sakti hain.", parse_mode="Markdown")


# ============================================================
# 5) RENAME & DELETE SYSTEM
# ============================================================
@dp.message(Command("rename"))
async def cmd_rename(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    import re
    # Support both formats:
    # /rename 'Holi 2026' 'Holi Shayari'   ← spaces wale naam (quotes mein)
    # /rename OldName NewName                   ← simple naam (bina quotes)
    parts = message.text.split(maxsplit=1)
    text = parts[1].strip() if len(parts) > 1 else ""

    quoted = re.findall(r"['\"](.+?)['\"]", text)
    if len(quoted) >= 2:
        old_name, new_name = quoted[0].strip(), quoted[1].strip()
    elif len(quoted) == 1:
        return await message.answer(
            "❌ Dono naam quotes mein likhein!\nExample: `/rename 'Holi 2026' 'Holi Shayari'`",
            parse_mode="Markdown"
        )
    else:
        simple = text.split()
        if len(simple) < 2:
            return await message.answer(
                "❌ **Usage:**\n• Space wale naam: `/rename 'Holi 2026' 'Holi Shayari'`\n• Simple naam: `/rename OldName NewName`",
                parse_mode="Markdown"
            )
        old_name, new_name = simple[0].strip(), simple[1].strip()

    if not old_name or not new_name:
        return await message.answer("❌ Naam khali nahi ho sakta!", parse_mode="Markdown")

    # Check new name conflict
    conflict = await albums_col.find_one({"name": {"$regex": f"^{new_name}$", "$options": "i"}})
    if conflict:
        return await message.answer(f"⚠️ **'{new_name}'** naam pehle se exist karta hai!", parse_mode="Markdown")

    album = await find_album(old_name)
    if not album:
        return await message.answer(f"❌ **'{old_name}'** naam ya ID ka album nahi mila.", parse_mode="Markdown")

    await albums_col.update_one(
        {"_id": album["_id"]},
        {"$set": {"name": new_name, "updated_at": now_ist()}}
    )
    await message.answer(f"📝 Album rename ho gaya!\n**{album['name']}** → **{new_name}**", parse_mode="Markdown")


@dp.message(Command("delete"))
async def cmd_delete(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/delete AlbumName`", parse_mode="Markdown")

    name = args[1].strip()

    # Find album by name OR ID
    album = await find_album(name)
    if not album:
        return await message.answer(f"❌ **'{name}'** naam ya ID ka album nahi mila.", parse_mode="Markdown")

    # Confirm delete with inline buttons
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="🗑️ Haan, Delete Karo", callback_data=f"del_yes_{album['album_id']}"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="del_no")
    )

    await message.answer(
        f"⚠️ **Delete Confirmation**\n\n"
        f"📁 Album: **{album['name']}**\n"
        f"🆔 ID: `{album['album_id']}`\n"
        f"🖼 Photos: {album['count']}\n\n"
        f"Kya aap sure hain? Yeh action **undo nahi** ho sakta!",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )


@dp.callback_query(F.data.startswith("del_"))
async def process_delete(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer("🚫 Access Denied!", show_alert=True)

    if callback.data == "del_no":
        await callback.answer("❌ Delete cancel kar diya.")
        await callback.message.edit_text("❌ **Delete operation cancel kar diya gaya.**", parse_mode="Markdown")
        return

    album_id = callback.data.replace("del_yes_", "")
    result = await albums_col.delete_one({"album_id": album_id})

    if result.deleted_count:
        await bot.send_message(
            STORAGE_CHANNEL,
            f"🗑️ **Album Deleted**\nID: `{album_id}`\nTime: {now_ist().strftime('%Y-%m-%d %H:%M')}",
            parse_mode="Markdown"
        )
        await callback.message.edit_text(
            f"🗑️ **Album successfully delete ho gaya!**\nID: `{album_id}`",
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text("❌ Delete nahi ho saka. Album pehle se delete tha?", parse_mode="Markdown")

    await callback.answer()


# ============================================================
# 6) SMART SEARCH SYSTEM
# ============================================================
@dp.message(Command("search"))
async def cmd_search(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/search <naam ya album_id>`", parse_mode="Markdown")

    query = args[1].strip()

    # Partial + Case-insensitive search on Name OR Album ID
    cursor = albums_col.find({
        "$or": [
            {"name": {"$regex": query, "$options": "i"}},
            {"album_id": {"$regex": query, "$options": "i"}}
        ]
    }).sort("created_at", -1).limit(10)

    results = await cursor.to_list(length=10)

    if not results:
        return await message.answer(
            f"🔍 **'{query}'** ke liye koi album nahi mila.\n"
            f"Saare albums dekhne ke liye `/albums` likhein.",
            parse_mode="Markdown"
        )

    response = f"🔍 **Search Results for '{query}':** ({len(results)} mila)\n\n"
    for alb in results:
        status_icon = "🔒" if alb.get("locked") else "🔓"
        created = alb.get("created_at", now_ist()).strftime("%d %b %Y")
        response += (
            f"{status_icon} **{alb['name']}**\n"
            f"   🆔 `{alb['album_id']}` | 🖼 {alb['count']} photos | 📅 {created}\n"
            f"   👁 `/view_{alb['album_id']}`\n\n"
        )

    await message.answer(response, parse_mode="Markdown")


# ============================================================
# 7) ALBUM LISTING SYSTEM
# ============================================================
@dp.message(Command("albums"))
async def cmd_list(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    try:
        cursor = albums_col.find().sort("created_at", -1)
        albums = await cursor.to_list(length=50)

        if not albums:
            return await message.answer(
                "📂 **Aapka Personal Cloud Khali Hai!**\n\nPehla album banane ke liye `/album <naam>` likhein.",
                parse_mode="Markdown"
            )

        total_photos = sum(a.get("count", 0) for a in albums)
        locked_count = sum(1 for a in albums if a.get("locked"))

        header = (
            f"☁️ **Personal Cloud Albums**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📊 {len(albums)} albums | 🖼 {total_photos} photos | 🔒 {locked_count} locked\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
        )

        lines = []
        for alb in albums:
            icon = "🔒" if alb.get("locked") else "📁"
            album_id = alb.get("album_id") or "N/A"
            name = alb.get("name") or "Unnamed"
            count = alb.get("count", 0)

            # View link sirf tab dikhao jab valid album_id ho
            if album_id != "N/A":
                view_link = f"   👁 `/view_{album_id}`"
            else:
                view_link = f"   ⚠️ ID missing (purana record)"

            lines.append(
                f"{icon} **{name}**\n"
                f"   🆔 `{album_id}` | 🖼 {count} photos\n"
                f"{view_link}"
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
            if chunk:
                await message.answer(chunk, parse_mode="Markdown")
        else:
            await message.answer(full_text, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"/albums error: {e}")
        await message.answer(f"❌ Albums load karte waqt error aaya:\n`{e}`", parse_mode="Markdown")


# ============================================================
# 8) STATS DASHBOARD
# ============================================================
@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    try:
        total_albums = await albums_col.count_documents({})
        locked_count = await albums_col.count_documents({"locked": True})
        unlocked_count = total_albums - locked_count

        pipeline = [{"$group": {"_id": None, "total": {"$sum": "$count"}}}]
        total_photos_result = await albums_col.aggregate(pipeline).to_list(1)
        total_photos = total_photos_result[0]["total"] if total_photos_result else 0

        # Latest album
        latest = await albums_col.find_one(sort=[("created_at", -1)])
        latest_name = latest["name"] if latest else "-"
        latest_time = latest["created_at"].strftime("%d %b %Y") if latest else "-"

        # Largest album
        largest = await albums_col.find_one(sort=[("count", -1)])
        largest_name = f"{largest['name']} ({largest['count']} photos)" if largest else "-"

        stats_text = (
            f"📊 **Personal Cloud - Stats Dashboard**\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📁 **Total Albums:** {total_albums}\n"
            f"🖼 **Total Photos:** {total_photos}\n"
            f"🔒 **Locked Albums:** {locked_count}\n"
            f"🔓 **Unlocked Albums:** {unlocked_count}\n\n"
            f"📅 **Latest Album:** {latest_name}\n"
            f"   Created: {latest_time}\n\n"
            f"🏆 **Largest Album:** {largest_name}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🟢 **Bot Status:** Online\n"
            f"🕐 **Checked:** {now_ist().strftime('%d %b %Y, %H:%M')}"
        )

        await message.answer(stats_text, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Stats error: {e}")
        await message.answer("❌ Stats laate waqt error aaya. MongoDB connection check karein.")


# ============================================================
# VIEW ALBUM - /view_<album_id>
# ============================================================
@dp.message(F.text.regexp(r"^/view_[A-Za-z0-9\-]+$"))
async def view_by_id(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    aid = message.text.replace("/view_", "").strip()
    album = await find_album(aid)

    if not album:
        return await message.answer(f"❌ Album ID **`{aid}`** nahi mila.", parse_mode="Markdown")

    await message.answer(
        f"📂 **{album['name']}**\n"
        f"🆔 `{album['album_id']}` | 🖼 {album['count']} photos\n\n"
        f"_Photos load ho rahi hain..._",
        parse_mode="Markdown"
    )

    photos = album.get("photos", [])
    sent = 0
    failed = 0

    # Send in media groups of 10 for efficiency
    for i in range(0, len(photos), 10):
        batch = photos[i:i+10]
        media_group = []
        for item in batch:
            if isinstance(item, dict):
                fid, mtype = item["file_id"], item["type"]
            else:
                fid, mtype = item, "photo"
            if mtype == "video":
                media_group.append(types.InputMediaVideo(media=fid))
            elif mtype == "document":
                media_group.append(types.InputMediaDocument(media=fid))
            else:
                media_group.append(types.InputMediaPhoto(media=fid))
        try:
            await bot.send_media_group(message.chat.id, media=media_group)
            sent += len(batch)
        except TelegramBadRequest as e:
            logger.error(f"Media group send error: {e}")
            for item in batch:
                fid = item["file_id"] if isinstance(item, dict) else item
                mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
                try:
                    if mtype == "video":
                        await bot.send_video(message.chat.id, fid)
                    elif mtype == "document":
                        await bot.send_document(message.chat.id, fid)
                    else:
                        await bot.send_photo(message.chat.id, fid)
                    sent += 1
                except:
                    failed += 1
        await asyncio.sleep(0.5)

    summary = f"✅ **{sent}/{len(photos)} photos** successfully bheji gayi!"
    if failed:
        summary += f"\n⚠️ {failed} photos send nahi ho sakin (expired file IDs)."
    await message.answer(summary, parse_mode="Markdown")


# ============================================================
# CANCEL - Cancel any active session
# ============================================================
@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message):
    uid = message.from_user.id
    if uid in user_sessions:
        session = user_sessions[uid]
        mode = session.get("mode", "unknown")
        name = session.get("name", "")
        del user_sessions[uid]
        await message.answer(
            f"❌ **Session Cancel Ho Gaya!**\n"
            f"Mode: {mode} | Album: {name}\n"
            f"_{len(session.get('photos', []))} unsaved photos discard ho gayi._",
            parse_mode="Markdown"
        )
    else:
        await message.answer("⚠️ Koi active session nahi hai cancel karne ke liye.")


# ============================================================
# GRANT / DENIED SYSTEM (Owner only)
# ============================================================
@dp.message(Command("grant"))
async def cmd_grant(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.answer("🚫 Sirf bot owner yeh command use kar sakta hai!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer(
            "❌ Usage:\n"
            "• `/grant 123456789` - User ID se\n"
            "• `/grant @username` - Username se (user ne pehle bot ko message kiya ho)",
            parse_mode="Markdown"
        )

    target = args[1].strip()

    # User ID directly diya
    if target.lstrip("-").isdigit():
        user_id = int(target)
        if user_id == ADMIN_ID:
            return await message.answer("⚠️ Aap pehle se owner hain!")

        granted_users.add(user_id)
        await db.granted_users.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "username": None, "granted_at": now_ist(), "granted_by": message.from_user.id}},
            upsert=True
        )
        await message.answer(
            f"✅ **Access Granted!**\n"
            f"🆔 User ID: `{user_id}`\n"
            f"Ab yeh user bot ke saare features use kar sakta hai.",
            parse_mode="Markdown"
        )
        # Greeting message to newly granted user
        try:
            now = now_ist()
            try:
                user_chat = await bot.get_chat(user_id)
                first_name = user_chat.first_name or "Friend"
            except:
                first_name = "Friend"
            await bot.send_message(
                user_id,
                f"👋 **HEY {first_name}!**\n\n"
                f"🎉 **Grant Access Successfully!**\n\n"
                f"🥳 **ENJOY!!**\n\n"
                f"📅 **Access Date:** {now.strftime('%d %B %Y')}\n"
                f"🕐 **Access Time:** {now.strftime('%I:%M %p')} IST",
                parse_mode="Markdown"
            )
            logger.info(f"✅ Greeting sent to {user_id}")
        except Exception as e:
            logger.warning(f"Could not send greeting to {user_id}: {e}")
            # Agar message nahi gaya to owner ko batao
            await message.answer(f"⚠️ User ko greeting nahi bheji ja saki. User ne pehle bot ko /start karna hoga.", parse_mode="Markdown")

    # @username diya
    elif target.startswith("@"):
        username = target.lstrip("@").lower()
        # DB mein dhundo agar pehle message kiya ho
        user_doc = await db.granted_users.find_one({"username": username})
        if user_doc and user_doc.get("user_id"):
            user_id = user_doc["user_id"]
            granted_users.add(user_id)
            await db.granted_users.update_one(
                {"user_id": user_id},
                {"$set": {"granted_at": now_ist(), "granted_by": message.from_user.id}},
                upsert=True
            )
            await message.answer(
                f"✅ **Access Granted!**\n"
                f"👤 @{username} | 🆔 `{user_id}`",
                parse_mode="Markdown"
            )
            # Greeting message to newly granted user
            try:
                now = now_ist()
                try:
                    user_chat = await bot.get_chat(user_id)
                    first_name = user_chat.first_name or "Friend"
                except:
                    first_name = username or "Friend"
                await bot.send_message(
                    user_id,
                    f"👋 **HEY {first_name}!**\n\n"
                    f"🎉 **Grant Access Successfully!**\n\n"
                    f"🥳 **ENJOY!!**\n\n"
                    f"📅 **Access Date:** {now.strftime('%d %B %Y')}\n"
                    f"🕐 **Access Time:** {now.strftime('%I:%M %p')}",
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.warning(f"Could not send greeting to {user_id}: {e}")
        else:
            # Username se grant kar do, jab pehli baar message karega tab activate hoga
            await db.granted_users.update_one(
                {"username": username},
                {"$set": {"username": username, "user_id": None, "granted_at": now_ist(), "granted_by": message.from_user.id, "pending": True}},
                upsert=True
            )
            await message.answer(
                f"⏳ **Pending Grant!**\n"
                f"👤 @{username} ko grant kar diya gaya.\n"
                f"Jab woh pehli baar bot ko message karenge, access activate ho jayega.\n\n"
                f"💡 _Tip: User ID use karna zyada reliable hai._",
                parse_mode="Markdown"
            )
    else:
        await message.answer("❌ Valid User ID ya @username dein.\nExample: `/grant 123456789` ya `/grant @john`", parse_mode="Markdown")


@dp.message(Command("denied"))
async def cmd_denied(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.answer("🚫 Sirf bot owner yeh command use kar sakta hai!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer(
            "❌ Usage:\n"
            "• `/denied 123456789` - User ID se\n"
            "• `/denied @username` - Username se",
            parse_mode="Markdown"
        )

    target = args[1].strip()

    if target.lstrip("-").isdigit():
        user_id = int(target)
        if user_id == ADMIN_ID:
            return await message.answer("⚠️ Owner ka access remove nahi kar sakte!")

        granted_users.discard(user_id)
        result = await db.granted_users.delete_one({"user_id": user_id})

        if result.deleted_count:
            await message.answer(
                f"🚫 **Access Removed!**\n"
                f"🆔 User ID: `{user_id}`\n"
                f"Ab yeh user bot use nahi kar sakta.",
                parse_mode="Markdown"
            )
        else:
            await message.answer(f"⚠️ User ID `{user_id}` granted list mein nahi tha.", parse_mode="Markdown")

    elif target.startswith("@"):
        username = target.lstrip("@").lower()
        user_doc = await db.granted_users.find_one({"username": username})
        if user_doc:
            if user_doc.get("user_id"):
                granted_users.discard(user_doc["user_id"])
            await db.granted_users.delete_one({"username": username})
            await message.answer(
                f"🚫 **Access Removed!**\n"
                f"👤 @{username} ab bot use nahi kar sakta.",
                parse_mode="Markdown"
            )
        else:
            await message.answer(f"⚠️ @{username} granted list mein nahi tha.", parse_mode="Markdown")
    else:
        await message.answer("❌ Valid User ID ya @username dein.", parse_mode="Markdown")


@dp.message(Command("grantlist"))
async def cmd_grantlist(message: types.Message):
    """Owner only - Saare granted users ki list"""
    if not is_owner(message.from_user.id):
        return await message.answer("🚫 Sirf bot owner yeh command use kar sakta hai!")

    cursor = db.granted_users.find()
    users = await cursor.to_list(length=100)

    if not users:
        return await message.answer("📋 Abhi koi granted user nahi hai.\n`/grant` se kisi ko access dein.", parse_mode="Markdown")

    text = "👥 **Granted Users List:**\n━━━━━━━━━━━━━━━━━━\n\n"
    for u in users:
        uid = u.get("user_id")
        uname = u.get("username")
        pending = u.get("pending", False)
        granted_at = u.get("granted_at", now_ist()).strftime("%d %b %Y")

        status = "⏳ Pending" if pending else "✅ Active"
        id_str = f"`{uid}`" if uid else "-"
        name_str = f"@{uname}" if uname else "-"

        text += f"{status}\n👤 {name_str} | 🆔 {id_str}\n📅 {granted_at}\n\n"

    text += f"━━━━━━━━━━━━━━━━━━\nTotal: {len(users)} users"
    await message.answer(text, parse_mode="Markdown")


# ============================================================
# /b2 - Send album to specific user
# ============================================================
@dp.message(Command("b2"))
async def cmd_b2(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    # Format: /b2 ALB-XXXXXX @username_or_userid
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.answer(
            "❌ **Usage:**\n"
            "`/b2 ALB-260306001824 @username`\n"
            "`/b2 ALB-260306001824 123456789`\n\n"
            "Album ID aur User ID/username dono zaroori hain.",
            parse_mode="Markdown"
        )

    album_identifier = args[1].strip()
    target = args[2].strip()

    # Find album by ID or name
    album = await find_album(album_identifier)

    if not album:
        return await message.answer(
            f"❌ Album **'{album_identifier}'** nahi mila.\n"
            f"ID ya naam sahi se likhein.",
            parse_mode="Markdown"
        )

    # Resolve target user
    target_id = None
    target_name = target

    if target.lstrip("-").isdigit():
        target_id = int(target)
        target_name = str(target_id)
    elif target.startswith("@"):
        username = target.lstrip("@").lower()
        # DB mein dhundo
        user_doc = await db.granted_users.find_one({"username": username})
        if user_doc and user_doc.get("user_id"):
            target_id = user_doc["user_id"]
            target_name = f"@{username}"
        else:
            return await message.answer(
                f"❌ **{target}** ka user ID nahi mila.\n"
                f"User ne pehle bot ko /start kiya ho, ya seedha User ID use karein.",
                parse_mode="Markdown"
            )
    else:
        return await message.answer("❌ Valid @username ya User ID dein.", parse_mode="Markdown")

    # Start sending
    photos = album.get("photos", [])
    if not photos:
        return await message.answer("❌ Is album mein koi file nahi hai.", parse_mode="Markdown")

    await message.answer(
        f"📤 **Sending album to {target_name}...**\n"
        f"📁 Album: **{album['name']}**\n"
        f"🖼 Files: {len(photos)}",
        parse_mode="Markdown"
    )

    # Send intro message to target user
    try:
        await bot.send_message(
            target_id,
            f"📂 **{album['name']}**\n"
            f"🖼 {len(photos)} files\n"
            f"_Sending..._",
            parse_mode="Markdown"
        )
    except Exception as e:
        return await message.answer(
            f"❌ User **{target_name}** ko message nahi bheji ja saki.\n"
            f"User ne bot ko pehle /start kiya ho.",
            parse_mode="Markdown"
        )

    sent = 0
    failed = 0

    for item in photos:
        if isinstance(item, dict):
            fid, mtype = item["file_id"], item["type"]
        else:
            fid, mtype = item, "photo"
        try:
            if mtype == "video":
                await bot.send_video(target_id, fid)
            elif mtype == "document":
                await bot.send_document(target_id, fid)
            elif mtype == "audio":
                await bot.send_audio(target_id, fid)
            elif mtype == "voice":
                await bot.send_voice(target_id, fid)
            else:
                await bot.send_photo(target_id, fid)
            sent += 1
        except Exception as e:
            logger.error(f"b2 send error: {e}")
            failed += 1
        await asyncio.sleep(0.3)

    # Summary to owner
    summary = (
        f"✅ **Album Sent!**\n"
        f"📁 {album['name']} → {target_name}\n"
        f"🖼 Sent: {sent}/{len(photos)}"
    )
    if failed:
        summary += f"\n⚠️ Failed: {failed}"
    await message.answer(summary, parse_mode="Markdown")

    # Summary to target user
    try:
        await bot.send_message(
            target_id,
            f"✅ **{sent} files** successfully receive ho gayi!",
            parse_mode="Markdown"
        )
    except:
        pass


# ============================================================
# /id - Get User & Chat ID (Everyone ke liye)
# ============================================================
@dp.message(Command("id"))
async def cmd_id(message: types.Message):
    user = message.from_user
    chat = message.chat
    uname = f"@{user.username}" if user.username else "N/A"
    user_info = (
        "\U0001f465 **Your Info:**\n"
        f"\U0001f194 User ID: `{user.id}`\n"
        f"\U0001f4db Name: {user.full_name}\n"
        f"\U0001f517 Username: {uname}\n\n"
        "\U0001f4ac **Chat Info:**\n"
        f"\U0001f194 Chat ID: `{chat.id}`\n"
        f"\U0001f4dd Chat Type: {chat.type}"
    )
    await message.answer(user_info, parse_mode="Markdown")


# ============================================================
# UNKNOWN COMMAND HANDLER
# ============================================================
@dp.message(F.text.startswith("/"))
async def unknown_command(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("YOU ARE NOT MY SENPAI 😤")

# ============================================================
# ERROR HANDLER
# ============================================================
@dp.error()
async def error_handler(event: types.ErrorEvent):
    logger.error(f"Unhandled error: {event.exception}", exc_info=True)


# ============================================================
# MAIN
# ============================================================
async def main():
    logger.info("🚀 Personal Cloud Bot starting...")
    try:
        # Verify MongoDB connection
        await client.admin.command("ping")
        logger.info("✅ MongoDB connected!")
        
        # Create indexes for faster search
        # sparse=True - null album_id wale purane documents ignore honge
        await albums_col.create_index([("name", 1)])
        await albums_col.create_index([("album_id", 1)], unique=True, sparse=True)
        await db.granted_users.create_index([("user_id", 1)])
        await db.granted_users.create_index([("username", 1)])

        # DB se granted users load karo memory mein (restart safe)
        granted_docs = await db.granted_users.find({"user_id": {"$ne": None}, "pending": {"$ne": True}}).to_list(length=500)
        for doc in granted_docs:
            if doc.get("user_id"):
                granted_users.add(doc["user_id"])
        logger.info(f"✅ {len(granted_users)} granted users loaded from DB!")
        
        logger.info("✅ Bot polling started!")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"❌ Startup error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
