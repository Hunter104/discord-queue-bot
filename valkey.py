import functools
import json
import os
import logging
from glide import GlideClientConfiguration, NodeAddress, GlideClient
from glide.glide import Script

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

_ADD_USER_SCRIPT = Script("""
    if server.call("lpos", KEYS[1], ARGV[1]) then
        return 1
    end
    server.call("lpush", KEYS[1], ARGV[1])
    return 0
""")

_config = None

def configure(host, port):
    global _config
    _config = GlideClientConfiguration([NodeAddress(host, port)], request_timeout=500)

# TODO: maybe not efficient
def with_client(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if _config is None:
            raise RuntimeError("Glide client not configured")
        client = await GlideClient.create(_config)
        result = await func(client, *args, **kwargs)
        await client.close()
        return result
    return wrapper

@with_client
async def addUser(client, username):
    # TODO: fix datatype here
    return await client.invoke_script(_ADD_USER_SCRIPT, ['queue'], [username]) == 0

@with_client
async def popBlocking(client):
    return (await client.brpop(['queue'], 0))[1]

@with_client
async def removeUser(client, user_id):
    await client.lrem('queue', 0, str(user_id))

@with_client
async def getSize(client):
    return await client.llen('queue')

@with_client
async def setHostStatus(client, name, status):
    return await client.hset(name, status)

@with_client
async def getHostStatus(client, name):
    raw = await client.hgetall(name)
    # decode keys and values
    return {k.decode(): v.decode() for k, v in raw.items()}

@with_client
async def sendNotification(client, data):
    return await client.lpush('notifs', [json.dumps(data)])

@with_client
async def popNotificationBlocking(client):
    data = (await client.brpop(['notifs'], 0))[1]
    return json.loads(data)

@with_client
async def getAll(client):
    raw = await client.lrange('queue', 0, -1)
    return [x.decode() for x in raw]