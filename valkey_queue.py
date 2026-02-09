import functools
import asyncio
import os
import logging
from dotenv import load_dotenv
from glide import Script, GlideClientConfiguration, NodeAddress, GlideClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

SUCCESS=1
_INSERT_SCRIPT=Script("""
    if redis.call('sismember', KEYS[2], ARGV[1]) == 1 then
        return 0
    else
        redis.call('lpush', KEYS[1], ARGV[1])
        redis.call('sadd', KEYS[2], ARGV[1])
        return 1
    end
""")

_POP_SCRIPT=Script("""
    local v = redis.call('rpop', KEYS[1])
    if not v then
        return nil
    end
    redis.call('srem', KEYS[2], v)
    return v
""")

_REMOVE_SCRIPT=Script("""
    redis.call('lrem', KEYS[1], 0, ARGV[1])
    redis.call('srem', KEYS[2], ARGV[1])
""")


load_dotenv()
_config = GlideClientConfiguration([NodeAddress(os.environ["VALKEY_HOST"], int(os.environ["VALKEY_PORT"])),], request_timeout=500)

def get_set_key(queue_id):
    return f'set:{queue_id}'

def get_queue_key(queue_id):
    return f'queue:{queue_id}'

def with_client(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        logger.info("Creating Glide client")
        queue_id = os.environ["QUEUE_ID"]
        client = await GlideClient.create(_config)
        result = await func(client, queue_id, *args, **kwargs)
        await client.close()
        return result
    return wrapper

@with_client
async def addUser(client, queue_id, user_id):
    logger.info(f"Adding user {user_id} to queue {queue_id}")
    return (await client.invoke_script(_INSERT_SCRIPT, keys=[get_queue_key(queue_id), get_set_key(queue_id)], args=[str(user_id)])) == SUCCESS

@with_client
async def popUser(client, queue_id):
    logger.info(f"Popping user from queue {queue_id}")
    return await client.invoke_script(_POP_SCRIPT, keys=[get_queue_key(queue_id), get_set_key(queue_id)],)

@with_client
async def removeUser(client, queue_id, user_id):
    logger.info(f"Removing user {user_id} from queue {queue_id}")
    await client.invoke_script(_REMOVE_SCRIPT, keys=[get_queue_key(queue_id), get_set_key(queue_id)], args=[str(user_id)])

@with_client
async def getSize(client, queue_id):
    return await client.llen(get_queue_key(queue_id))

@with_client
async def getAll(client, queue_id):
    logger.info(f"Getting all users in queue {queue_id}")
    result = await client.lrange(get_queue_key(queue_id), 0, -1)
    return [int(uid) for uid in result]

async def main():
    res = await getSize()
    print(res)

if __name__ == "__main__":
    asyncio.run(main())
