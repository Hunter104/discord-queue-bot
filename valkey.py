import datetime
import datetime
import enum
import functools
import logging
from typing import Callable, Awaitable, Concatenate, ParamSpec, TypeVar

import msgspec.json
from glide import GlideClientConfiguration, NodeAddress, GlideClient
from glide.glide import Script
from glide_shared import ExpirySet, ExpiryType, Batch

import common
from common import PopNotification, SlotData, HeartbeatData

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

_POP_USER_SCRIPT = Script("""
    -- Move user to processing queue atomically
    local res = server.call("lmove", KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
    if not res then
        return nil
    end
    
    -- Set user as assigned to host
    server.call("set", KEYS[3], ARGV[1])
    
    return res
""")

_ADD_USER_SCRIPT = Script("""
    -- If user is already in queue then skip
    if server.call("lpos", KEYS[1], ARGV[1]) then
        return 1
    end
    
    -- If users isn't free then skip
    if server.call("get", KEYS[2]) then
        return 2
    end
    
    server.call("lpush", KEYS[1], ARGV[1])
    return 0
""")

_config: GlideClientConfiguration | None = None

class AddReturnCode(enum.Enum):
    ALREADY_IN_QUEUE = 1
    ALREADY_ASSIGNED = 2
    SUCCESS = 0

def configure(host, port):
    global _config
    _config = GlideClientConfiguration([NodeAddress(host, port)], request_timeout=500)
    logger.info(f"Configured client for {host}:{port}")

P = ParamSpec('P')
R = TypeVar('R')

_NOTIFICATIONS_QUEUE_KEY = 'notifs'
_WAITING_QUEUE_KEY = 'waiting_queue'

_HOST_KEY_PREFIX = 'rpi'
_PROCESSING_QUEUE_KEY_SUFFIX = 'processing'
_HOST_SLOT_KEY_SUFFIX = 'slot'
_HOST_HEARTBEAT_SUFFIX = 'heartbeat'

_USER_KEY_PREFIX = 'user'
_USER_ASSIGNMENT_KEY_SUFFIX = 'assigned_to'

def get_host_slot_key(hostname: str) -> str:
    return f'{_HOST_KEY_PREFIX}:{hostname}:{_HOST_SLOT_KEY_SUFFIX}'

def get_processing_queue_key(hostname: str) -> str:
    return f'{_HOST_KEY_PREFIX}:{hostname}:{_PROCESSING_QUEUE_KEY_SUFFIX}'

def get_user_assigned_host_key(username: str) -> str:
    return f'{_USER_KEY_PREFIX}:{username}:{_USER_ASSIGNMENT_KEY_SUFFIX}'

def get_host_heartbeat(hostname: str):
    return f'{_HOST_KEY_PREFIX}:{hostname}:{_HOST_HEARTBEAT_SUFFIX}'


# TODO: maybe not efficient
def with_client(func: Callable[Concatenate[GlideClient, P], Awaitable[R]]) -> Callable[[P], Awaitable[R]]:
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if _config is None:
            raise RuntimeError("Glide client not configured")
        client = await GlideClient.create(_config)
        result = await func(client, *args, **kwargs)
        await client.close()
        return result
    return wrapper


########################
#  FRONTEND FUNCTIONS  #
########################

@with_client
async def add_user(client: GlideClient, username: str) -> AddReturnCode:
    ret = await client.invoke_script(
        _ADD_USER_SCRIPT,
        [_WAITING_QUEUE_KEY, get_user_assigned_host_key(username)],
        [username])
    return AddReturnCode(ret)

@with_client
async def remove_waiting_user(client: GlideClient, user_id: int):
    await client.lrem(_WAITING_QUEUE_KEY, 0, str(user_id))

@with_client
async def get_waiting_queue_size(client: GlideClient):
    return await client.llen(_WAITING_QUEUE_KEY)

@with_client
async def peek_processing_queue(client: GlideClient, hostname: str) -> str | None:
    res =  await client.lindex(get_processing_queue_key(hostname), 0)
    if res is None:
        return None
    return res.decode()

@with_client
async def get_all_waiting_users(client: GlideClient) -> list[str]:
    raw = await client.lrange(_WAITING_QUEUE_KEY, 0, -1)
    return [x.decode() for x in raw]

@with_client
async def pop_notification_blocking(client: GlideClient) -> PopNotification | None:
    raw = await client.brpop([_NOTIFICATIONS_QUEUE_KEY], 0)
    if raw is None:
        return None
    return msgspec.json.decode(raw[1].decode(), type=PopNotification)

@with_client
async def get_all_heartbeats(client: GlideClient) -> list[common.HeartbeatData]:
    heartbeats = []
    cursor = '0'
    while True:
        cursor, vals = await client.scan(cursor, f'{_HOST_KEY_PREFIX}:*:{_HOST_HEARTBEAT_SUFFIX}')
        if vals is None or len(vals) == 0:
            break
        decoded_vals = [val.decode() for val in vals]
        raw_heartbeats = await client.mget(decoded_vals)
        heartbeats.extend([msgspec.json.decode(raw.decode(), type=HeartbeatData) for raw in raw_heartbeats])

        if cursor.decode() == '0':
            break

    return heartbeats


@with_client
async def get_heartbeat(client: GlideClient, hostname: str) -> common.HeartbeatData | None:
    raw = await client.get(get_host_heartbeat(hostname))
    if raw is None:
        return None
    return msgspec.json.decode(raw.decode(), type=common.HeartbeatData)

####################
#  HOST FUNCTIONS  #
####################

@with_client
async def pop_waiting(client:GlideClient, hostname: str) -> str | None:
    res = await client.invoke_script(
        _POP_USER_SCRIPT,
        keys=[_WAITING_QUEUE_KEY, get_processing_queue_key(hostname), get_user_assigned_host_key(hostname)],
        args=[hostname])

    if res is None:
        return None

    return res.decode()

# TODO: maybe not clean code
@with_client
async def finish_processing(client: GlideClient, hostname: str, user: str, expiry: datetime.datetime):
    """Mark the end of processing user by host, sends notification to bot, removes user from the processing queue, and fills in occupied slot data"""
    transaction = Batch(is_atomic=True)

    occupied_slot = SlotData(user, expiry)
    notif = PopNotification(hostname, user)

    # Set host as occupied
    transaction.set(get_host_slot_key(hostname),msgspec.json.encode(occupied_slot))

    # send notification
    transaction.lpush(_NOTIFICATIONS_QUEUE_KEY, [msgspec.json.encode(notif)])

    # remove user from processing queue
    transaction.lrem(get_processing_queue_key(hostname), 0, hostname)

    await client.exec(transaction, True)

@with_client
async def get_slot(client: GlideClient, hostname: str) -> SlotData | None:
    raw = await client.get(get_host_slot_key(hostname))
    if raw is None:
        return None
    return msgspec.json.decode(raw.decode(), type=SlotData)

@with_client
async def release_user(client: GlideClient, hostname: str):
    transaction = Batch(is_atomic=True)
    transaction.delete([get_host_slot_key(hostname)])
    transaction.delete([get_user_assigned_host_key(hostname)])
    await client.exec(transaction, True)

@with_client
async def send_notification(client: GlideClient, data: PopNotification):
    return await client.lpush(_NOTIFICATIONS_QUEUE_KEY, [msgspec.json.encode(data)])

@with_client
async def send_heartbeat(client: GlideClient, data: common.HeartbeatData, expiry: datetime.timedelta):
    return await client.set(get_host_heartbeat(data.hostname),
                            msgspec.json.encode(data),
                            expiry=ExpirySet(expiry_type=ExpiryType.SEC, value=expiry))