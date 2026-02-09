import discord
import os
import logging
from dotenv import load_dotenv
from valkey_queue import addUser, removeUser, getAll

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()
bot = discord.Bot()

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')

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
    if queue:
        queue_list = "\n".join([f"{idx + 1}. <@{user_id}>" for idx, user_id in enumerate(queue)])
        await ctx.respond(f"Current Queue:\n{queue_list}")
    else:
        await ctx.respond("The queue is currently empty.")

bot.run(os.environ["BOT_TOKEN"])
