import discord
import os
import logging
import json
from discord.ext import tasks
from dotenv import load_dotenv

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
    logger.info("Waiting for notification...")
    data = await valkey.popNotificationBlocking()
    # TODO: temporary, get discord user from unix via db query
    logger.info(f"Sending notification to user {data}")
    user = await bot.fetch_user(int(data["user"]))
    await user.send(f'You have been assigned to host {data["name"]}')

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')
    read_notifications.start()

@bot.slash_command(name="join_queue", description="Join the queue")
async def join_queue(ctx):
    user_id = ctx.author.id
    if await addUser(user_id):
        await ctx.respond("You have been added to the queue.")
    else:
        await ctx.respond("You are already in the queue.")

@bot.slash_command(name="leave_queue", description="Leave the queue")
async def leave_queue(ctx):
    user_id = ctx.author.id
    if await removeUser(user_id):
        await ctx.respond("You have been removed from the queue.")
    else:
        await ctx.respond("You are not in the queue.")

@bot.slash_command(name="queue_status", description="Check the queue status")
async def queue_status(ctx):
    queue = await getAll()

    if not queue:
        await ctx.respond("The queue is currently empty.")
        return

    lines = []
    for idx, user_id in enumerate(queue, start=1):
        user = bot.get_user(user_id)

        if user is None:
            try:
                user = await bot.fetch_user(user_id)
            except discord.NotFound:
                lines.append(f"{idx}. ❌ Unknown user (`{user_id}`)")
                await removeUser(user_id)
                continue
            except discord.HTTPException:
                lines.append(f"{idx}. ⚠️ Error fetching user (`{user_id}`)")
                continue

        lines.append(f"{idx}. {user.name} ({user.mention})")

    await ctx.respond("Current Queue:\n" + "\n".join(lines))

@bot.slash_command(name="remove_from_queue", description="Admin: remove a user from the queue")
@discord.default_permissions(administrator=True)
async def remove_from_queue(ctx, user: discord.User):
    if await removeUser(user.id):
        await ctx.respond(f"✅ {user.mention} has been removed from the queue.")
    else:
        await ctx.respond(f"⚠️ {user.mention} is not in the queue.")

load_dotenv()
valkey.configure(os.environ["VALKEY_HOST"], int(os.environ["VALKEY_PORT"]))
bot.run(os.environ["BOT_TOKEN"])
