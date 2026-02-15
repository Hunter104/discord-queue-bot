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
intents = discord.Intents.default()
intents.members = True

bot = discord.Bot(intents=intents)

TEMPLATE_FILE = "status_template.json.jinja"
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

        discord_ids = [discord_id_by_hostname.get(unix_user) for unix_user in waiting_unix_users]

        template_vars = {
            'hosts': await asyncio.gather(*host_string_tasks),
            'queue': discord_ids,
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
            channel = bot.get_channel(channelId)
            if channel is None:
                channel = await bot.fetch_channel(channelId)
            message = await channel.fetch_message(messageId)
            await message.edit(embeds=[embed])
        except discord.NotFound:
            logger.exception(
                "Status message %s in channel %s not found, removing from database.",
                messageId,
                channelId,
            )
            await bot_db.remove_status_message(channelId)


@tasks.loop(seconds=1)
async def read_notifications():
    async with get_connection(config) as conn:
        data = await conn.pop_notification_blocking()

    logger.info("Got notification: %s", data)
    unix_user = data.unix_user
    discord_id = await get_discord_id(unix_user)
    if discord_id is None:
        logger.error("Unix user %s not registered.", unix_user)
        return

    discord_user = bot.get_user(discord_id)
    if discord_user is None:
        logger.error("Could not find discord user for id %s", discord_id)
        return

    # TODO: talvez mandar num canal específico
    await discord_user.send(f'You have been assigned to host {data.hostname}.')


@bot.event
async def on_ready():
    logger.info("Logged in as %s", bot.user)

    logger.info("Starting notification reading loop")
    read_notifications.start()

    logger.info("Starting status message update loop")
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
async def remove_from_queue(ctx, discord_user: discord.User):
    unix_user = await get_unix_user(discord_user.id)
    if discord_user is None:
        await ctx.respond(f"User {discord_user} is not registered, please contact your administrator")

    async with get_connection(config) as conn:
        if await conn.remove_waiting_user(unix_user):
            await ctx.respond(f"✅ {unix_user.mention} has been removed from the queue.")
        else:
            await ctx.respond(f"⚠️ {unix_user.mention} is not in the queue.")


@bot.slash_command(name="register_user", description="Admin: register user into the system")
@discord.default_permissions(manage_guild=True)
async def register_user(ctx, user: discord.User, unix_user: str):
    await register_user(user.id, unix_user)
    ctx.respond("User registered successfully.")


async def pretty_format_host(data: HeartbeatData) -> str:
    match data.status:
        case HostStatus.IN_USE:
            user_id = await get_discord_id(data.current_user)
            return f"🔴 {data.hostname} (last seen: {data.timestamp.strftime('%H:%M:%S')}) - In use by <@{user_id}> until {data.expiry.strftime('%H:%M:%S')}"
        case HostStatus.AWAITING:
            return f"🟢 {data.hostname} (last seen: {data.timestamp.strftime('%H:%M:%S')}) - Available"


@bot.slash_command(name="create_status_message", description="Admin: create a status message in this channel")
@discord.default_permissions(manage_guild=True)
async def create_status_message(ctx):
    embed = await generate_embed()
    msg = await ctx.channel.send(embeds=[embed])
    await bot_db.register_status_message(msg.channel.id, msg.id)
    await ctx.respond("Message created successfully.")


@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    await ctx.respond("There was an error while processing your request, please contact your administrator")
    raise error


bot.run(os.environ["BOT_TOKEN"])
