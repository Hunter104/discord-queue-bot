from datetime import datetime, timedelta
import enum
import logging
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Dict

from glide import GlideClient
from glide.glide import Script
from glide_shared import Batch

from common.valkey_defs import *

logger = logging.getLogger(__name__)

class AddReturnCode(enum.Enum):
    ALREADY_IN_QUEUE = 1
    ALREADY_ASSIGNED = 2
    IN_PROCESSING = 3
    SUCCESS = 0

_ADD_USER_SCRIPT = Script("""
    -- If user is already in queue then skip
    if server.call("lpos", KEYS[1], ARGV[1]) then
        return 1
    end

    -- If users isn't free then skip
    if server.call("get", KEYS[2]) then
        return 2
    end

    -- If user is in processing queue then skip
    if server.call("lpos", KEYS[3], ARGV[1]) then
        return 3
    end

    server.call("lpush", KEYS[1], ARGV[1])
    return 0
""")

@dataclass
class HostStatus:
    hostname: str
    is_occupied: bool
    expiry: datetime | None
    current_user: str | None
    last_timestamp: datetime

    @classmethod
    def from_hgetall(cls, data: dict[bytes, bytes]) -> "HostStatus":
        is_occupied = bool(int(data[b"is_occupied"].decode()))
        if is_occupied:
            expiry = datetime.fromtimestamp(int(data[b"expiry"].decode()))
            current_user = data[b"current_user"].decode()
        else:
            expiry = None
            current_user = None
        return cls(
            hostname=data[b"hostname"].decode(),
            is_occupied=is_occupied,
            expiry=expiry,
            current_user=current_user,
            last_timestamp=datetime.fromtimestamp(int(data[b"last_timestamp"])),
        )

async def add_user(client: GlideClient, username: str) -> AddReturnCode:
    ret = await client.invoke_script(
        _ADD_USER_SCRIPT,
        [WAITING_QUEUE_KEY, get_user_assigned_to_host_key(username), PROCESSING_QUEUE_KEY],
        [username],
    )
    return AddReturnCode(ret)


async def register_user(client: GlideClient, discord_id: int, unix_user: str):
    transaction = Batch(is_atomic=True)
    transaction.set(get_user_discord_id_key(unix_user), str(discord_id))
    transaction.set(get_user_unix_id_key(str(discord_id)), unix_user)
    await client.exec(transaction, True)


async def get_discord_id(client: GlideClient, unix_user: str) -> str | None:
    res = await client.get(get_user_discord_id_key(unix_user))
    if res is None:
        return None
    return res.decode()


async def get_unix_user(client: GlideClient, discord_id: int) -> str | None:
    # TODO: switch id from string to numeric
    res = await client.get(get_user_unix_id_key(str(discord_id)))
    if res is None:
        return None
    return res.decode()


async def register_status_message(client: GlideClient, channel_id: int, message_id: int):
    transaction = Batch(is_atomic=True)
    transaction.sadd(get_status_message_channels_key(), [str(channel_id)])
    transaction.sadd(get_status_message_channel_list_key(channel_id), [str(message_id)])
    await client.exec(transaction, True)


async def delete_status_message(client: GlideClient, channel_id: int, message_id: int):
    transaction = Batch(is_atomic=True)
    transaction.srem(get_status_message_channel_list_key(channel_id), [message_id.to_bytes()])
    transaction.srem(get_status_message_channels_key(), [channel_id.to_bytes()])
    await client.exec(transaction, True)


async def get_status_messages(client: GlideClient) -> AsyncGenerator[tuple[int, int], Any]:
    channels = await client.smembers(get_status_message_channels_key())
    for channel in channels:
        key = get_status_message_channel_list_key(int(channel.decode()))
        messages = await client.smembers(key)
        for message in messages:
            yield int(channel.decode()), int(message.decode())


async def remove_waiting_user(client: GlideClient, user: str):
    await client.lrem(WAITING_QUEUE_KEY, 0, user)


async def get_all_waiting_users(client: GlideClient, max_users: int = 5) -> list[str]:
    raw = await client.lrange(WAITING_QUEUE_KEY, 0, max_users - 1)
    return [x.decode() for x in raw]


async def get_all_hosts(client: GlideClient) -> Dict[str, HostStatus | None]:
    data = {}
    hosts = await client.smembers(REGISTERED_HOSTS_KEY)
    for host in hosts:
        hostname = host.decode()
        raw = await client.hgetall(get_host_status_key(hostname))
        if not raw:
            data[hostname] = None
        else:
            data[hostname] = HostStatus.from_hgetall(raw)
    return data


async def requeue_timed_out_users(client: GlideClient):
    users = await client.lrange(PROCESSING_QUEUE_KEY, 0, -1)
    for user in users:
        user = user.decode()
        lock = get_user_lock_key(user)
        if await client.get(lock) is None:
            transaction = Batch(is_atomic=True)
            transaction.lrem(PROCESSING_QUEUE_KEY, 0, user)
            transaction.lpush(WAITING_QUEUE_KEY, [user])
            await client.exec(transaction, True)




async def deregister_host(client: GlideClient, hostname: str):
    transaction = Batch(is_atomic=True)
    transaction.srem(REGISTERED_HOSTS_KEY, [hostname])
    transaction.delete([get_host_status_key(hostname)])
    await client.exec(transaction, True)

