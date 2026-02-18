import datetime
import enum
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import glide
from glide import GlideClientConfiguration, GlideClient
from glide.glide import Script
from glide_shared import ExpirySet, ExpiryType, Batch
from google.protobuf.timestamp_pb2 import Timestamp

import protocol_pb2
from protocol_pb2 import HeartbeatData, HostStatus, PopNotification, SlotData

logger = logging.getLogger(__name__)


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


class AddReturnCode(enum.Enum):
    ALREADY_IN_QUEUE = 1
    ALREADY_ASSIGNED = 2
    IN_PROCESSING = 3
    SUCCESS = 0


_STATUS_MESSAGE_LIST_KEY = 'status_messages'
_NOTIFICATIONS_QUEUE_KEY = 'notifs'
_WAITING_QUEUE_KEY = 'waiting_queue'
# Users in processing will wait in this queue and will be removed when processing is finished,
# a process will be responsible for removing old users
_PROCESSING_QUEUE_KEY = 'processing_queue'
_LOCK_PREFIX = 'lock:'

# TODO: maybe change this into a hash too
_HOST_KEY_PREFIX = 'rpi'
_HOST_SLOT_KEY_SUFFIX = 'slot'
_HOST_HEARTBEAT_SUFFIX = 'heartbeat'

# TODO: change this into a hash
_USER_KEY_PREFIX = 'user'
_USER_ASSIGNMENT_KEY_SUFFIX = 'assigned_to'
_USER_DISCORD_ID_SUFFIX = 'discord_id'
_USER_UNIX_ID_SUFFIX = 'unix_id'

_DISCORD_CHANNEL_PREFIX = 'channel'
_STATUS_MESSAGE_CHANNEL_LIST_KEY = 'messages'
_STATUS_MESSAGE_CHANNELS_KEY = 'status_message_channels'

def get_user_lock_key(username: str) -> str:
    return f'{_LOCK_PREFIX}:{username}'

def get_status_message_channels_key() -> str:
    return f'{_STATUS_MESSAGE_LIST_KEY}:{_STATUS_MESSAGE_CHANNELS_KEY}'

def get_status_message_channel_list_key(channel_id: int) -> str:
    return f'{_DISCORD_CHANNEL_PREFIX}:{channel_id}:{_STATUS_MESSAGE_CHANNEL_LIST_KEY}'

def get_user_discord_id_key(username: str) -> str:
    return f'{_USER_KEY_PREFIX}:{username}:{_USER_DISCORD_ID_SUFFIX}'

def get_user_unix_id_key(discord_id: str) -> str:
    return f'{_USER_KEY_PREFIX}:{discord_id}:{_USER_UNIX_ID_SUFFIX}'

def get_host_slot_key(hostname: str) -> str:
    return f'{_HOST_KEY_PREFIX}:{hostname}:{_HOST_SLOT_KEY_SUFFIX}'


def get_user_assigned_host_key(username: str) -> str:
    return f'{_USER_KEY_PREFIX}:{username}:{_USER_ASSIGNMENT_KEY_SUFFIX}'


def get_host_heartbeat(hostname: str):
    return f'{_HOST_KEY_PREFIX}:{hostname}:{_HOST_HEARTBEAT_SUFFIX}'


@asynccontextmanager
async def get_connection(config: GlideClientConfiguration):
    client = await GlideClient.create(config)
    try:
        yield ValkeyConnection(client)
    finally:
        await client.close()


# TODO: User persistent connection per process instead of per code block
class ValkeyConnection:
    def __init__(self, client: GlideClient):
        if client is None:
            raise ValueError("Client cannot be None")
        self.client: GlideClient = client

    async def add_user(self, username: str) -> AddReturnCode:
        ret = await self.client.invoke_script(
            _ADD_USER_SCRIPT,
            [_WAITING_QUEUE_KEY, get_user_assigned_host_key(username), _PROCESSING_QUEUE_KEY],
            [username],
        )
        return AddReturnCode(ret)

    ########################
    #  FRONTEND FUNCTIONS  #
    ########################

    # --- User Registration ---
    async def register_user(self, discord_id: int, unix_user: str):
        transaction = Batch(is_atomic=True)
        transaction.set(get_user_discord_id_key(unix_user), str(discord_id))
        transaction.set(get_user_unix_id_key(str(discord_id)), unix_user)
        await self.client.exec(transaction, True)

    async def get_discord_id(self, unix_user: str) -> str | None:
        res = await self.client.get(get_user_discord_id_key(unix_user))
        if res is None:
            return None
        return res.decode()

    async def get_unix_user(self, discord_id: str) -> str | None:
        res = await self.client.get(get_user_unix_id_key(discord_id))
        if res is None:
            return None
        return res.decode()

    # --- Status Messages ---

    async def register_status_message(self, channel_id: int, message_id: int):
        transaction = Batch(is_atomic=True)
        transaction.sadd(get_status_message_channels_key(), [str(channel_id)])
        transaction.sadd(get_status_message_channel_list_key(channel_id), [str(message_id)])
        await self.client.exec(transaction, True)

    async def delete_status_message(self, channel_id: int, message_id: int):
        transaction = Batch(is_atomic=True)
        transaction.srem(get_status_message_channel_list_key(channel_id), [message_id.to_bytes()])
        transaction.srem(get_status_message_channels_key(), [channel_id.to_bytes()])
        await self.client.exec(transaction, True)

    async def get_status_messages(self) -> AsyncGenerator[tuple[int, int], Any]:
        channels = await self.client.smembers(get_status_message_channels_key())
        for channel in channels:
            key = get_status_message_channel_list_key(int(channel.decode()))
            messages = await self.client.smembers(key)
            for message in messages:
                yield int(channel.decode()), int(message.decode())

    async def remove_waiting_user(self, user_id: int):
        await self.client.lrem(_WAITING_QUEUE_KEY, 0, str(user_id))

    async def get_all_waiting_users(self, max: int = 5) -> list[str]:
        raw = await self.client.lrange(_WAITING_QUEUE_KEY, 0, max-1)
        return [x.decode() for x in raw]

    async def pop_notification_blocking(self) -> PopNotification | None:
        raw = await self.client.brpop([_NOTIFICATIONS_QUEUE_KEY], 0)
        if raw is None:
            return None
        pop_notification = PopNotification()
        pop_notification.ParseFromString(raw[1])
        return pop_notification

    # TODO: maybe change heartbeats to stream
    async def get_all_heartbeats(self) -> list[HeartbeatData]:
        heartbeats = []
        cursor = '0'
        while True:
            cursor, keys = await self.client.scan(cursor, f'{_HOST_KEY_PREFIX}:*:{_HOST_HEARTBEAT_SUFFIX}')
            if keys is None or len(keys) == 0:
                break
            decoded_keys = [key.decode() for key in keys]
            heartbeats = []
            for raw in await self.client.mget(decoded_keys):
                heartbeat = HeartbeatData()
                heartbeat.ParseFromString(raw)
                heartbeats.append(heartbeat)

            if cursor.decode() == '0':
                break

        return heartbeats

    async def requeue_timed_out_users(self):
        users = await self.client.lrange(_PROCESSING_QUEUE_KEY, 0, -1)
        for user in users:
            user = user.decode()
            lock = get_user_lock_key(user)
            # User has expired lock
            if await self.client.get(lock) is None:
                # Reinsert at the beggining of waiting queue
                transaction = Batch(is_atomic=True)
                transaction.lrem(_PROCESSING_QUEUE_KEY, 0, user)
                transaction.lpush(_WAITING_QUEUE_KEY, [user])
                await self.client.exec(transaction, True)


    ####################
    #  HOST FUNCTIONS  #
    ####################

    async def pop_waiting_queue_blocking(self) -> str | None:
        res = await self.client.blmove(_WAITING_QUEUE_KEY, _PROCESSING_QUEUE_KEY, glide.ListDirection.LEFT, glide.ListDirection.RIGHT, timeout=0)

        if res is None:
            return None

        user = res.decode()
        # TODO: set client timeout as parameter
        await self.client.set(get_user_lock_key(user), "doodoo", expiry=ExpirySet(expiry_type=ExpiryType.SEC, value=5),)
        return res.decode()

    async def finish_processing(self, hostname: str, user: str, expiry: datetime.datetime):
        transaction = Batch(is_atomic=True)

        occupied_slot = SlotData()
        occupied_slot.current_user = user
        timestamp = Timestamp()
        timestamp.FromDatetime(datetime.datetime.now())
        occupied_slot.expiry.FromDatetime(expiry)

        notif = PopNotification()
        notif.hostname = hostname
        notif.unix_user = user

        # Persist current slot data (in case of daemon failure)
        transaction.set(
            get_host_slot_key(hostname),
            occupied_slot.SerializeToString(),
            expiry=ExpirySet(
                expiry_type=ExpiryType.UNIX_MILLSEC,
                value=expiry
            )
        )

        # Mark user as occupied
        transaction.set(
            get_user_assigned_host_key(user),
            hostname,
            expiry=ExpirySet(
                expiry_type=ExpiryType.UNIX_MILLSEC,
                value=expiry
            )
        )

        # Send notification
        transaction.lpush(
            _NOTIFICATIONS_QUEUE_KEY,
            [notif.SerializeToString()]
        )

        # Remove user from processing queue
        transaction.lrem(
            _PROCESSING_QUEUE_KEY,
            0,
            hostname
        )

        await self.client.exec(transaction, True)

    async def get_slot(self, hostname: str) -> SlotData | None:
        raw = await self.client.get(get_host_slot_key(hostname))
        if raw is None:
            return None
        slot = SlotData()
        slot.ParseFromString(raw)
        return slot

    async def release_user(self, hostname: str):
        transaction = Batch(is_atomic=True)
        transaction.delete([get_host_slot_key(hostname)])
        transaction.delete([get_user_assigned_host_key(hostname)])
        await self.client.exec(transaction, True)

    async def send_heartbeat(
            self,
            hostname: str,
            status: HostStatus,
            expiry: datetime.datetime | None,
            current_user: str | None,
            timestamp: datetime.datetime,
            message_expiry: datetime.timedelta,
    ):
        data = HeartbeatData()
        data.hostname = hostname
        data.status = status
        if current_user is not None:
            data.current_user = current_user
        if expiry is not None:
            data.expiry.FromDatetime(expiry)
        data.timestamp.FromDatetime(timestamp)
        return await self.client.set(
            get_host_heartbeat(data.hostname),
            data.SerializeToString(),
            expiry=ExpirySet(
                expiry_type=ExpiryType.SEC,
                value=message_expiry
            )
        )
