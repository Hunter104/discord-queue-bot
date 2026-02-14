import aiosqlite
import os


async def get_unix_user(discord_user_id):
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        async with conn.execute("SELECT unixUser FROM Users WHERE discordId = ? LIMIT 1;", (discord_user_id,)) as cursor:
            result = await cursor.fetchone()
            if result is not None:
                return result[0]
            else:
                return None


async def get_discord_id(unix_user):
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        async with conn.execute("SELECT discordId FROM Users WHERE unixUser = ? LIMIT 1;", (unix_user,)) as cursor:
            result = await cursor.fetchone()
            if result is not None:
                return result[0]
            else:
                return None

async def register_user(discord_user_id, unix_user):
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        await conn.execute("INSERT INTO Users (discordId, unixUser) VALUES (?, ?);", (discord_user_id, unix_user))
        await conn.commit()

async def register_status_message(channel_id, message_id):
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        await conn.execute("INSERT INTO StatusMessages (channelId, messageId) VALUES (?, ?);", (channel_id, message_id))
        await conn.commit()

async def get_status_messages():
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        async with conn.execute("SELECT channelId, messageId FROM StatusMessages;") as cursor:
            return await cursor.fetchall()

async def remove_status_message(channel_id):
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        await conn.execute("DELETE FROM StatusMessages WHERE channelId = ?", (channel_id,))
        await conn.commit()