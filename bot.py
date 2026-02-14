import asyncio
from datetime import datetime

import aiosqlite
import discord
import os
import logging
import json

import jinja2
from discord.ext import tasks
from dotenv import load_dotenv

import bot_db
from bot_db import getDiscordId, getUnixUser, registerUser

import valkey
from valkey import add_user, remove_waiting_user, get_all_waiting_users

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()
bot = discord.Bot()

notification_task = None

TEMPLATE_FILE="status_template.json.jinja"

templateLoader = jinja2.FileSystemLoader(searchpath=".")
templateEnv = jinja2.Environment(loader=templateLoader)

template = templateEnv.get_template(TEMPLATE_FILE)

@tasks.loop(seconds=1)
async def read_notifications():
    data = await valkey.pop_notification_blocking()
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
    # update_status_messages.start()

@bot.slash_command(name="join_queue", description="Join the queue")
async def join_queue(ctx):
    unix_user = await getUnixUser(ctx.author.id)
    if unix_user is None:
        await ctx.respond("You have not been registered, please contact your administrator")
        return

    ret = await add_user(unix_user)
    match ret:
        case valkey.AddReturnCode.ALREADY_IN_QUEUE:
            await ctx.respond("You are already in the queue.")
        case valkey.AddReturnCode.ALREADY_ASSIGNED:
            await ctx.respond("You are already assigned to a host.")
        case valkey.AddReturnCode.SUCCESS:
            await ctx.respond("You have been added to the queue.")

@bot.slash_command(name="leave_queue", description="Leave the queue")
async def leave_queue(ctx):
    unix_user = await getUnixUser(ctx.author.id)
    if unix_user is None:
        await ctx.respond("You have not been registered, please contact your administrator")
        return
    if await remove_waiting_user(unix_user):
        await ctx.respond("You have been removed from the queue.")
    else:
        await ctx.respond("You are not in the queue.")

# @bot.slash_command(name="host_status", description="Check status of all hosts")
# async def host_status(ctx):
#     hosts = await valkey.getHosts()
#     print(hosts)

@bot.slash_command(name="queue_status", description="Check the queue status")
async def queue_status(ctx):
    queue = await get_all_waiting_users()

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
                await remove_waiting_user(user_id)
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

    if await remove_waiting_user(unix_user):
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

# async def generateEmbed():
#     hostnames = await valkey.getHosts()
#     host_tasks = [valkey.get_heartbeat(host) for host in hostnames]
#     template_vars = {
#         'hosts': await asyncio.gather(*host_tasks),
#         'queue': await valkey.getAll(),
#         'now': datetime.now(),
#     }
#     logger.debug(template_vars)
#     embed_json = template.render(template_vars)
#     return discord.Embed.from_dict(json.loads(embed_json))
#
# @tasks.loop(seconds=3)
# async def update_status_messages():
#     logger.info("Updating status messages")
#     status_messages = await bot_db.getStatusMessages()
#     embed = await generateEmbed()
#     for channelId, messageId in status_messages:
#         channel = await bot.fetch_channel(channelId)
#         message = await channel.fetch_message(messageId)
#         await message.edit(embeds=[embed])
#
# @bot.slash_command(name="create_status_message", description="Admin: create a status message in this channel")
# @discord.default_permissions(manage_guild=True)
# async def createStatusMessage(ctx):
#     embed = await generateEmbed()
#     try:
#         msg = await ctx.channel.send(embeds=[embed])
#         await bot_db.registerStatusMessage(msg.channel.id, msg.id)
#         await ctx.respond("Message created succesfully")
#     except aiosqlite.Error as e:
#         ctx.respond(f"Error creating message: {e}")
#         logger.error(f"Error creating message: {e}")


load_dotenv()
valkey.configure(os.environ["VALKEY_HOST"], int(os.environ["VALKEY_PORT"]))
bot.run(os.environ["BOT_TOKEN"])
