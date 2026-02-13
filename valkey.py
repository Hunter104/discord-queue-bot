import functools
import json
import os
import logging
from glide import GlideClientConfiguration, NodeAddress, GlideClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
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
        logger.info("Creating Glide client")
        client = await GlideClient.create(_config)
        result = await func(client, *args, **kwargs)
        await client.close()
        return result
    return wrapper

#TODO: fetch unix user from user id and push that instead
@with_client
async def addUser(client, username):
    logger.info(f"Adding user {username} to queue")
    # TODO: fix datatype here
    if username not in await getAll():
        return await client.lpush('queue', [str(username)])
    else:
        return None

@with_client
async def popBlocking(client):
    logger.info("Popping user from queue ")
    return (await client.brpop(['queue'], 0))[1]

@with_client
async def removeUser(client, user_id):
    logger.info(f"Removing user {user_id} from queue")
    await client.lrem('queue', 0, str(user_id))

@with_client
async def getSize(client):
    logger.info("Getting queue size")
    return await client.llen('queue')

@with_client
async def setHostStatus(client, name, status):
    logger.info(f"Setting host status for {name}: {status}")
    return await client.hset(name, status)

@with_client
async def getHostStatus(client, name):
    logger.info(f"Getting host status for {name}")
    raw = await client.hgetall(name)
    # decode keys and values
    return {k.decode(): v.decode() for k, v in raw.items()}

@with_client
async def sendNotification(client, data):
    logger.info(f"Sending notification: {data}")
    return await client.lpush('notifs', [json.dumps(data)])

@with_client
async def popNotificationBlocking(client):
    logger.info("Popping notification from queue ")
    data = (await client.brpop(['notifs'], 0))[1]
    return json.loads(data)

@with_client
async def getAll(client):
    logger.info(f"Getting all users in queue")
    raw = await client.lrange('queue', 0, -1)
    return [x.decode() for x in raw]