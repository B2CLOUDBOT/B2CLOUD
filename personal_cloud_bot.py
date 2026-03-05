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
        "started_at": datetime.now()
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
# PHOTO HANDLER (for create & add modes)
# ============================================================
@dp.message(F.photo)
async def handle_photo(message: types.Message):
    uid = message.from_user.id
    if uid not in user_sessions:
        return  # Silently ignore if no active session

    session = user_sessions[uid]
    photo = message.photo[-1]  # Highest resolution
    unique_id = photo.file_unique_id

    # 1) & 3) Duplicate Photo Detection
    if unique_id in session["ids"]:
        return await message.reply("🚫 **Duplicate photo!** Isse skip kar diya gaya.")

    session["photos"].append(photo.file_id)
    session["ids"].add(unique_id)

    count = len(session["photos"])
    # Feedback every 5 photos or on first photo
    if count == 1 or count % 5 == 0:
        await message.reply(
            f"✅ Photo #{count} add ho gayi!\n"
            f"Bas bhejte rahein... /close ya /save_add se finish karein."
        )


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

    auto_id = f"ALB-{datetime.now().strftime('%y%m%d%H%M')}"
    duration = (datetime.now() - session["started_at"]).seconds // 60

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
        await bot.send_photo(
            message.chat.id,
            session["photos"][0],
            caption=preview_caption,
            reply_markup=builder.as_markup(),
            parse_mode="Markdown"
        )
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
        album_id = f"ALB-{datetime.now().strftime('%y%m%d%H%M%S')}"
        album_doc = {
            "album_id": album_id,
            "name": session["name"],
            "photos": session["photos"],
            "count": len(session["photos"]),
            "locked": False,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }

        try:
            await albums_col.insert_one(album_doc)

            # 9) Backup to Storage Channel
            user = callback.from_user
            user_info = f"@{user.username}" if user.username else f"ID: {user.id}"

            # Step 1: Album creation info message
            await bot.send_message(
                STORAGE_CHANNEL,
                f"📁 **Create Album**\n"
                f"Name: {session['name']}\n"
                f"Created by: {user_info}",
                parse_mode="Markdown"
            )

            # Step 2: Send all photos to channel
            photos = session['photos']
            for i in range(0, len(photos), 10):
                batch = photos[i:i+10]
                media_group = [types.InputMediaPhoto(media=fid) for fid in batch]
                try:
                    await bot.send_media_group(STORAGE_CHANNEL, media=media_group)
                except Exception as ex:
                    logger.error(f"Channel photo send error: {ex}")
                await asyncio.sleep(0.3)

            # Step 3: Summary message with photo range
            await bot.send_message(
                STORAGE_CHANNEL,
                f"✅ **Album Saved & Stored**\n"
                f"🆔 ID: `{album_id}`\n"
                f"📁 Name: {session['name']}\n"
                f"🖼 Photos: {len(photos)}\n"
                f"🕐 Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                f"Chat ID: 1 to {len(photos)}",
                parse_mode="Markdown"
            )


            await callback.message.edit_caption(
                caption=f"✅ **Album Saved Successfully!**\n\n"
                        f"📁 Name: **{session['name']}**\n"
                        f"🆔 ID: `{album_id}`\n"
                        f"🖼 Photos: {len(session['photos'])}\n"
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
    album = await albums_col.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}})

    if not album:
        return await message.answer(
            f"❌ **'{name}'** naam ka album nahi mila.\n"
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
        "started_at": datetime.now()
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
                "$set": {"updated_at": datetime.now()}
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
            f"📁 **Photo Added**\n"
            f"Name: {session['name']}\n"
            f"Created by: {user_info}",
            parse_mode="Markdown"
        )

        # Step 2: Send only new photos to channel
        new_photos = session['photos']
        for i in range(0, len(new_photos), 10):
            batch = new_photos[i:i+10]
            media_group = [types.InputMediaPhoto(media=fid) for fid in batch]
            try:
                await bot.send_media_group(STORAGE_CHANNEL, media=media_group)
            except Exception as ex:
                logger.error(f"Channel add photo error: {ex}")
            await asyncio.sleep(0.3)

        # Step 3: Summary with photo range
        await bot.send_message(
            STORAGE_CHANNEL,
            f"➕ **Photos Added**\n"
            f"📁 Album: {session['name']}\n"
            f"🆔 ID: `{session['album_id']}`\n"
            f"🖼 Added: {len(session['photos'])} photos\n"
            f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
            f"Chat ID: {start_num} to {end_num}",
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
    result = await albums_col.update_one(
        {"name": {"$regex": f"^{name}$", "$options": "i"}},
        {"$set": {"locked": True, "updated_at": datetime.now()}}
    )

    if result.matched_count:
        await message.answer(f"🔒 Album **'{name}'** lock ho gaya!\nAb koi photo add nahi ki ja sakti.", parse_mode="Markdown")
    else:
        await message.answer(f"❌ Album **'{name}'** nahi mila.", parse_mode="Markdown")


@dp.message(Command("unlock"))
async def cmd_unlock(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/unlock AlbumName`", parse_mode="Markdown")

    name = args[1].strip()
    result = await albums_col.update_one(
        {"name": {"$regex": f"^{name}$", "$options": "i"}},
        {"$set": {"locked": False, "updated_at": datetime.now()}}
    )

    if result.matched_count:
        await message.answer(f"🔓 Album **'{name}'** unlock ho gaya!\nAb photos add ki ja sakti hain.", parse_mode="Markdown")
    else:
        await message.answer(f"❌ Album **'{name}'** nahi mila.", parse_mode="Markdown")


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

    result = await albums_col.update_one(
        {"name": {"$regex": f"^{old_name}$", "$options": "i"}},
        {"$set": {"name": new_name, "updated_at": datetime.now()}}
    )

    if result.matched_count:
        await message.answer(f"📝 Album rename ho gaya!\n**{old_name}** → **{new_name}**", parse_mode="Markdown")
    else:
        await message.answer(f"❌ **'{old_name}'** naam ka album nahi mila.", parse_mode="Markdown")


@dp.message(Command("delete"))
async def cmd_delete(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/delete AlbumName`", parse_mode="Markdown")

    name = args[1].strip()

    # Find album first for confirmation
    album = await albums_col.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}})
    if not album:
        return await message.answer(f"❌ **'{name}'** naam ka album nahi mila.", parse_mode="Markdown")

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
            f"🗑️ **Album Deleted**\nID: `{album_id}`\nTime: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
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
        created = alb.get("created_at", datetime.now()).strftime("%d %b %Y")
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
            f"💾 **Storage:** MongoDB Atlas + Telegram Channel\n"
            f"🕐 **Checked:** {datetime.now().strftime('%d %b %Y, %H:%M')}"
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
    album = await albums_col.find_one({"album_id": aid})

    if not album:
        return await message.answer(f"❌ Album ID **`{aid}`** nahi mila.", parse_mode="Markdown")

    if album.get("locked"):
        return await message.answer(
            f"🔒 **'{album['name']}'** album locked hai!\n"
            f"Pehle `/unlock {album['name']}` karein.",
            parse_mode="Markdown"
        )

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
        media_group = [types.InputMediaPhoto(media=fid) for fid in batch]
        try:
            await bot.send_media_group(message.chat.id, media=media_group)
            sent += len(batch)
        except TelegramBadRequest as e:
            logger.error(f"Media group send error: {e}")
            # Fallback: send one by one
            for fid in batch:
                try:
                    await bot.send_photo(message.chat.id, fid)
                    sent += 1
                except:
                    failed += 1
        await asyncio.sleep(0.5)  # Rate limit protection

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
            {"$set": {"user_id": user_id, "username": None, "granted_at": datetime.now(), "granted_by": message.from_user.id}},
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
            now = datetime.now()
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
                f"🕐 **Access Time:** {now.strftime('%I:%M %p')}",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"Could not send greeting to {user_id}: {e}")

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
                {"$set": {"granted_at": datetime.now(), "granted_by": message.from_user.id}},
                upsert=True
            )
            await message.answer(
                f"✅ **Access Granted!**\n"
                f"👤 @{username} | 🆔 `{user_id}`",
                parse_mode="Markdown"
            )
            # Greeting message to newly granted user
            try:
                now = datetime.now()
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
                {"$set": {"username": username, "user_id": None, "granted_at": datetime.now(), "granted_by": message.from_user.id, "pending": True}},
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
        granted_at = u.get("granted_at", datetime.now()).strftime("%d %b %Y")

        status = "⏳ Pending" if pending else "✅ Active"
        id_str = f"`{uid}`" if uid else "-"
        name_str = f"@{uname}" if uname else "-"

        text += f"{status}\n👤 {name_str} | 🆔 {id_str}\n📅 {granted_at}\n\n"

    text += f"━━━━━━━━━━━━━━━━━━\nTotal: {len(users)} users"
    await message.answer(text, parse_mode="Markdown")


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
