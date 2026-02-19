import asyncio
import json
import logging
import os
from datetime import datetime

import discord
import jinja2
from discord.ext import tasks
from dotenv import load_dotenv
from glide import GlideClient
from glide_shared import NodeAddress, GlideClientConfiguration

import valkey_bot as valkey

from common.proto.protocol_pb2 import HostStatus


class ValkeyBot(discord.Bot):
    client: GlideClient


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()
intents = discord.Intents.default()
intents.members = True

bot = ValkeyBot(intents=intents)

TEMPLATE_FILE = "templates/status_template.json.jinja"
templateLoader = jinja2.FileSystemLoader(searchpath=".")
templateEnv = jinja2.Environment(loader=templateLoader)
template = templateEnv.get_template(TEMPLATE_FILE)

config = GlideClientConfiguration([NodeAddress(os.environ["VALKEY_HOST"], int(os.environ["VALKEY_PORT"]))])


async def generate_embed():
    heartbeats = await valkey.get_all_hosts(bot.client)
    host_string_tasks = [pretty_format_host(name, host) for name, host in heartbeats]
    waiting_unix_users = await valkey.get_all_waiting_users(bot.client)

    discord_ids = [await valkey.get_discord_id(bot.client, unix_user) for unix_user in waiting_unix_users]

    template_vars = {
        'hosts': await asyncio.gather(*host_string_tasks),
        'queue': discord_ids,
        'now': datetime.now(),
    }
    logger.debug(template_vars)
    embed_json = template.render(template_vars)
    return discord.Embed.from_dict(json.loads(embed_json))


@tasks.loop(seconds=1)
async def monitor_processing_queue():
    await valkey.requeue_timed_out_users(bot.client)


@tasks.loop(seconds=1)
async def update_status_messages():
    embed = await generate_embed()
    async for channelId, messageId in valkey.get_status_messages(bot.client):
        try:
            channel = bot.get_channel(channelId)
            if channel is None:
                channel = await bot.fetch_channel(channelId)
            message = await channel.fetch_message(messageId)
            await message.edit(embeds=[embed])
        except discord.NotFound:
            logger.warning(
                "Status message %s in channel %s not found, removing from database.",
                messageId,
                channelId,
            )
            await valkey.delete_status_message(bot.client, channelId, messageId)


@tasks.loop(seconds=1)
async def read_notifications():
    data = await valkey.pop_notification_blocking(bot.client)

    logger.info("Got notification: %s", data)
    unix_user = data.unix_user
    discord_id = await valkey.get_discord_id(bot.client, unix_user)
    if discord_id is None:
        logger.error("Unix user %s not registered.", unix_user)
        return

    discord_user = bot.get_user(int(discord_id))
    if discord_user is None:
        logger.error("Could not find discord user for id %s", discord_id)
        return

    # TODO: talvez mandar num canal específico
    await discord_user.send(f'You have been assigned to host {data.hostname}.')


@bot.event
async def on_ready():
    logger.info("Logged in as %s", bot.user)
    logger.info("Starting valkey client")
    bot.client = await GlideClient.create(config)

    logger.info("Starting notification reading loop")
    read_notifications.start()

    logger.info("Starting status message update loop")
    update_status_messages.start()

    logger.info("Starting processing queue monitor loop")
    monitor_processing_queue.start()


@bot.slash_command(name="join_queue", description="Join the queue")
async def join_queue(ctx):
    unix_user = await valkey.get_unix_user(bot.client, ctx.author.id)
    if unix_user is None:
        logger.warning("Non registered user %d tried entering queue", ctx.author.id)
        await ctx.respond("You have not been registered, please contact your administrator")
        return

    ret = await valkey.add_user(bot.client, unix_user)
    match ret:
        case valkey.AddReturnCode.ALREADY_IN_QUEUE:
            logger.warning("User %s tried entering queue twice", unix_user)
            await ctx.respond("You are already in the queue.")
        case valkey.AddReturnCode.ALREADY_ASSIGNED:
            logger.warning("User %s tried entering queue while assigned", unix_user)
            await ctx.respond("You are already assigned to a host.")
        case valkey.AddReturnCode.IN_PROCESSING:
            logger.warning("User %s tried entering queue while in processing", unix_user)
            await ctx.respond("You are in awaiting processing by one of ours hosts, please wait.")
        case valkey.AddReturnCode.SUCCESS:
            await ctx.respond("You have been added to the queue.")


@bot.slash_command(name="leave_queue", description="Leave the queue")
async def leave_queue(ctx):
    unix_user = await valkey.get_unix_user(bot.client, ctx.author.id)
    if unix_user is None:
        await ctx.respond("You have not been registered, please contact your administrator")
        return

    if await valkey.remove_waiting_user(bot.client, unix_user):
        await ctx.respond("You have been removed from the queue.")
    else:
        await ctx.respond("You are not in the queue.")


@bot.slash_command(name="remove_from_queue", description="Admin: remove a user from the queue")
@discord.default_permissions(manage_guild=True)
async def remove_from_queue(ctx, discord_user: discord.User):
    unix_user = await valkey.get_unix_user(bot.client, discord_user.id)
    if unix_user is None:
        logger.warning("Attempt at removing non registered user %d from queue", discord_user.id)
        await ctx.respond("User not registered.")
        return

    if await valkey.remove_waiting_user(bot.client, unix_user):
        await ctx.respond(f"✅ <@{discord_user.id}> has been removed from the queue.")
    else:
        logger.warning("Attempt at removing non-present user %s from queue", unix_user)
        await ctx.respond(f"⚠️ <@{discord_user.id}> is not in the queue.")


@bot.slash_command(name="register_user", description="Admin: register user into the system")
@discord.default_permissions(manage_guild=True)
async def register_user(ctx, user: discord.User, unix_user: str):
    logger.info("Registering user %s with unix user %s", user.id, unix_user)
    await valkey.register_user(bot.client, user.id, unix_user)
    await ctx.respond("User registered successfully.")


async def pretty_format_host(host: str, data: HostStatus | None) -> str:
    if data is None:
        return f"⚠️ {host} Unavaliable"
    real_stamp = data.last_heartbeat.ToDatetime()
    if data.is_occupied:
        user_id = await valkey.get_discord_id(bot.client, data.current_user)
        real_expiry = data.expiry.ToDatetime()
        return f"🔴 {data.hostname} (last seen: {real_stamp.strftime('%H:%M:%S')}) - In use by <@{user_id}> until {real_expiry.strftime('%H:%M:%S')}"
    return f"🟢 {data.hostname} (last seen: {real_stamp.strftime('%H:%M:%S')}) - Available"


@bot.slash_command(name="create_status_message", description="Admin: create a status message in this channel")
@discord.default_permissions(manage_guild=True)
async def create_status_message(ctx):
    embed = await generate_embed()
    msg = await ctx.channel.send(embeds=[embed])
    await valkey.register_status_message(bot.client, msg.channel.id, msg.id)
    await ctx.respond("Message created successfully.")


@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    await ctx.respond("There was an error while processing your request, please contact your administrator")
    raise error


bot.run(os.environ["BOT_TOKEN"])
