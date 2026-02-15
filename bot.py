import asyncio
import json
import logging
import os
from datetime import datetime

import aiosqlite
import discord
import jinja2
from discord.ext import tasks
from dotenv import load_dotenv
from glide_shared import NodeAddress, GlideClientConfiguration

import bot_db
import valkey
from bot_db import get_discord_id, get_unix_user, register_user
from common import HeartbeatData, HostStatus
from valkey import get_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()
bot = discord.Bot(intents=discord.Intents.all())

TEMPLATE_FILE="status_template.json.jinja"
templateLoader = jinja2.FileSystemLoader(searchpath=".")
templateEnv = jinja2.Environment(loader=templateLoader)
template = templateEnv.get_template(TEMPLATE_FILE)

config = GlideClientConfiguration([NodeAddress(os.environ["VALKEY_HOST"], int(os.environ["VALKEY_PORT"]))])

async def generate_embed():
    async with get_connection(config) as valkey_conn:
        heartbeats = await valkey_conn.get_all_heartbeats()
        host_string_tasks = [pretty_format_host(host) for host in heartbeats]
        waiting_unix_users = await valkey_conn.get_all_waiting_users()
        discord_id_by_hostname = await bot_db.get_discord_id_by_hostname()

        waiting_discord_users = [discord_id_by_hostname.get(unix_user) for unix_user in waiting_unix_users]

        template_vars = {
            'hosts': await asyncio.gather(*host_string_tasks),
            'queue': waiting_discord_users,
            'now': datetime.now(),
        }
    logger.debug(template_vars)
    embed_json = template.render(template_vars)
    return discord.Embed.from_dict(json.loads(embed_json))

@tasks.loop(seconds=1)
async def update_status_messages():
    status_messages = await bot_db.get_status_messages()
    embed = await generate_embed()
    for channelId, messageId in status_messages:
        try:
            # Check the cache first to minimize rate limit risk
            channel = bot.get_channel(channelId)
            if channel is None:
                channel = await bot.fetch_channel(channelId)
            message = await channel.fetch_message(messageId)
            await message.edit(embeds=[embed])
        except discord.NotFound as e:
            logger.error(f"Status message {messageId} in channel {channelId} not found: {e}, removing from database.")
            await bot_db.remove_status_message(channelId)
        except Exception as e:
            logger.error(f"Error updating status message {messageId} in channel {channelId}: {e}")
            logger.error(f"Current variables: embed={embed}, status_messages={status_messages},")

@tasks.loop(seconds=1)
async def read_notifications():
    async with get_connection(config) as conn:
        data = await conn.pop_notification_blocking()

    # TODO: temporary, get discord user from unix via db query
    logger.info(f"Got notification: {data}")
    unix_user = data.unix_user
    discord_id = await get_discord_id(unix_user)
    if discord_id is None:
        logger.error(f"Unix user {unix_user} not registered.")
        return

    discord_user = bot.get_user(discord_id)
    if discord_user is None:
        logger.error(f"Could not find discord user for id {discord_id}")
        return

    await discord_user.send(f'You have been assigned to host {data.hostname}.')

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')

    logger.info('Starting notification reading loop')
    read_notifications.start()

    logger.info('Starting status message update loop')
    update_status_messages.start()

@bot.slash_command(name="join_queue", description="Join the queue")
async def join_queue(ctx):
    unix_user = await get_unix_user(ctx.author.id)
    if unix_user is None:
        await ctx.respond("You have not been registered, please contact your administrator")
        return

    async with get_connection(config) as conn:
        ret = await conn.add_user(unix_user)
        match ret:
            case valkey.AddReturnCode.ALREADY_IN_QUEUE:
                await ctx.respond("You are already in the queue.")
            case valkey.AddReturnCode.ALREADY_ASSIGNED:
                await ctx.respond("You are already assigned to a host.")
            case valkey.AddReturnCode.SUCCESS:
                await ctx.respond("You have been added to the queue.")

@bot.slash_command(name="leave_queue", description="Leave the queue")
async def leave_queue(ctx):
    unix_user = await get_unix_user(ctx.author.id)
    if unix_user is None:
        await ctx.respond("You have not been registered, please contact your administrator")
        return

    async with get_connection(config) as conn:
        if await conn.remove_waiting_user(unix_user):
            await ctx.respond("You have been removed from the queue.")
        else:
            await ctx.respond("You are not in the queue.")

@bot.slash_command(name="remove_from_queue", description="Admin: remove a user from the queue")
@discord.default_permissions(manage_guild=True)
async def remove_from_queue(ctx, unix_user: discord.User):
    unix_user = await get_discord_id(unix_user.id)
    if unix_user is None:
        await ctx.respond(f"User {unix_user} is not registered, please contact your administrator")

    async with get_connection(config) as conn:
        if await conn.remove_waiting_user(unix_user):
            await ctx.respond(f"✅ {unix_user.mention} has been removed from the queue.")
        else:
            await ctx.respond(f"⚠️ {unix_user.mention} is not in the queue.")

@bot.slash_command(name="register_user", description="Admin: register user into the system")
@discord.default_permissions(manage_guild=True)
async def register_user(ctx, user: discord.User, unix_user: str):
    try:
        await register_user(user.id, unix_user)
        ctx.respond("User registered successfully.")
    except aiosqlite.Error as e:
        ctx.respond(f"Error registering user: {e}")
        logger.error(f"Error registering user: {e}")

async def pretty_format_host(data: HeartbeatData) -> str:
    match data.status:
        case HostStatus.IN_USE:
            user_id = await get_discord_id(data.current_user)
            # REQUIRES GUILD MEMBERS INTENT
            discord_user = bot.get_user(user_id)
            return f"🔴 {data.hostname} (last seen: {data.timestamp.strftime('%H:%M:%S')}) - In use by {discord_user.mention} until {data.expiry.strftime('%H:%M:%S')}"
        case HostStatus.AWAITING:
            return f"🟢 {data.hostname} (last seen: {data.timestamp.strftime('%H:%M:%S')}) - Available"

@bot.slash_command(name="create_status_message", description="Admin: create a status message in this channel")
@discord.default_permissions(manage_guild=True)
async def create_status_message(ctx):
    embed = await generate_embed()
    try:
        msg = await ctx.channel.send(embeds=[embed])
        await bot_db.register_status_message(msg.channel.id, msg.id)
        await ctx.respond("Message created successfully.")
    except aiosqlite.Error as e:
        ctx.respond(f"Error creating message: {e}")
        logger.error(f"Error creating message: {e}")

@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    await ctx.respond(f"There was an error while processing your request, please contact your administrator")
    raise error  # Here we raise other errors to ensure they aren't ignored

bot.run(os.environ["BOT_TOKEN"])
