import aiosqlite
import os


async def getUnixUser(discord_user_id):
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        async with conn.execute("SELECT unixUser FROM Users WHERE discordId = ? LIMIT 1;", (discord_user_id,)) as cursor:
            result = await cursor.fetchone()
            if result is not None:
                return result[0]
            else:
                return None


async def getDiscordId(unix_user):
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        async with conn.execute("SELECT discordId FROM Users WHERE unixUser = ? LIMIT 1;", (unix_user,)) as cursor:
            result = await cursor.fetchone()
            if result is not None:
                return result[0]
            else:
                return None

async def registerUser(discord_user_id, unix_user):
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        await conn.execute("INSERT INTO Users (discordId, unixUser) VALUES (?, ?);", (discord_user_id, unix_user))
        await conn.commit()

async def registerStatusMessage(channelId, messageId):
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        await conn.execute("INSERT INTO StatusMessages (channelId, messageId) VALUES (?, ?);", (channelId, messageId))
        await conn.commit()

async def getStatusMessages():
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        async with conn.execute("SELECT channelId, messageId FROM StatusMessages;") as cursor:
            return await cursor.fetchall()

async def removeStatusMessage(channelId):
    async with aiosqlite.connect(os.environ["DB_PATH"]) as conn:
        await conn.execute("DELETE FROM StatusMessages WHERE channelId = ?", (channelId,))
        await conn.commit()