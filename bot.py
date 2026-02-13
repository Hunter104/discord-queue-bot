import aiosqlite
import discord
import os
import logging
import json
from discord.ext import tasks
from dotenv import load_dotenv
from bot_db import getDiscordId, getUnixUser, registerUser

import valkey
from valkey import addUser, removeUser, getAll

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()
bot = discord.Bot()

notification_task = None

@tasks.loop(seconds=1)
async def read_notifications():
    data = await valkey.popNotificationBlocking()
    # TODO: temporary, get discord user from unix via db query
    logger.info(f"Received notification: {data}")
    unix_user = data.unix_user
    discord_id = await getDiscordId(unix_user)
    if discord_id is None:
        logger.error(f"Unix user {unix_user} not registered.")
        return
    discord_user = await bot.fetch_user(discord_id)
    if discord_user is None:
        logger.error(f"Could not find discord user for id {discord_id}")
        return
    await discord_user.send(f'You have been assigned to host {data.hostname}.')

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')
    read_notifications.start()

@bot.slash_command(name="join_queue", description="Join the queue")
async def join_queue(ctx):
    unix_user = await getUnixUser(ctx.author.id)
    if unix_user is None:
        await ctx.respond("You have not been registered, please contact your administrator")
        return

    if await addUser(unix_user):
        await ctx.respond("You have been added to the queue.")
    else:
        await ctx.respond("You are already in the queue.")

@bot.slash_command(name="leave_queue", description="Leave the queue")
async def leave_queue(ctx):
    unix_user = await getUnixUser(ctx.author.id)
    if unix_user is None:
        await ctx.respond("You have not been registered, please contact your administrator")
        return
    if await removeUser(unix_user):
        await ctx.respond("You have been removed from the queue.")
    else:
        await ctx.respond("You are not in the queue.")

@bot.slash_command(name="host_status", description="Check status of all hosts")
async def host_status(ctx):
    hosts = await valkey.getHosts()
    print(hosts)

@bot.slash_command(name="queue_status", description="Check the queue status")
async def queue_status(ctx):
    queue = await getAll()

    if not queue:
        await ctx.respond("The queue is currently empty.")
        return

    # TODO: Create an sql query for this unoptimized loop
    lines = []
    for idx, user_id in enumerate(queue, start=1):
        discord_id = await getDiscordId(user_id)
        user = bot.get_user(discord_id)

        if user is None:
            try:
                user = await bot.fetch_user(discord_id)
            except discord.NotFound:
                lines.append(f"{idx}. ❌ Unknown user (`{discord_id}`)")
                await removeUser(user_id)
                continue
            except discord.HTTPException:
                lines.append(f"{idx}. ⚠️ Error fetching user (`{discord_id}`)")
                continue

        lines.append(f"{idx}. {user.name} ({user.mention})")

    await ctx.respond("Current Queue:\n" + "\n".join(lines))

@bot.slash_command(name="remove_from_queue", description="Admin: remove a user from the queue")
@discord.default_permissions(manage_guild=True)
async def remove_from_queue(ctx, unix_user: discord.User):
    unix_user = await getDiscordId(unix_user.id)
    if unix_user is None:
        await ctx.respond(f"User {unix_user.mention} is not registered, please contact your administrator")

    if await removeUser(unix_user):
        await ctx.respond(f"✅ {unix_user.mention} has been removed from the queue.")
    else:
        await ctx.respond(f"⚠️ {unix_user.mention} is not in the queue.")

@bot.slash_command(name="register", description="Admin: register user into the system")
@discord.default_permissions(manage_guild=True)
async def register(ctx, user: discord.User, unix_user: str):
    try:
        await registerUser(user.id, unix_user)
        ctx.respond("User registered successfully.")
    except aiosqlite.Error as e:
        ctx.respond(f"Error registering user: {e}")
        logger.error(f"Error registering user: {e}")

@bot.slash_command(name="create_status_message", description="Admin: create a status message in this channel")
@discord.default_permissions(manage_guild=True)
async def register(ctx, user: discord.User, unix_user: str):
    try:
        await registerUser(user.id, unix_user)
        ctx.respond("User registered successfully.")
    except aiosqlite.Error as e:
        ctx.respond(f"Error registering user: {e}")
        logger.error(f"Error registering user: {e}")


load_dotenv()
valkey.configure(os.environ["VALKEY_HOST"], int(os.environ["VALKEY_PORT"]))
bot.run(os.environ["BOT_TOKEN"])
