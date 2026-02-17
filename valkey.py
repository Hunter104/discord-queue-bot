import datetime
import enum
import logging
from contextlib import asynccontextmanager

from glide import GlideClientConfiguration, GlideClient
from glide.glide import Script
from glide_shared import ExpirySet, ExpiryType, Batch
from google.protobuf.timestamp_pb2 import Timestamp

from protocol_pb2 import HeartbeatData, HostStatus, PopNotification, SlotData

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


class AddReturnCode(enum.Enum):
    ALREADY_IN_QUEUE = 1
    ALREADY_ASSIGNED = 2
    SUCCESS = 0


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


@asynccontextmanager
async def get_connection(config: GlideClientConfiguration):
    client = await GlideClient.create(config)
    try:
        yield ValkeyConnection(client)
    finally:
        await client.close()


class ValkeyConnection:
    def __init__(self, client: GlideClient):
        if client is None:
            raise ValueError("Client cannot be None")
        self.client: GlideClient = client

    async def add_user(self, username: str) -> AddReturnCode:
        ret = await self.client.invoke_script(
            _ADD_USER_SCRIPT,
            [_WAITING_QUEUE_KEY, get_user_assigned_host_key(username)],
            [username],
        )
        return AddReturnCode(ret)

    ########################
    #  FRONTEND FUNCTIONS  #
    ########################

    async def remove_waiting_user(self, user_id: int):
        await self.client.lrem(_WAITING_QUEUE_KEY, 0, str(user_id))

    async def get_waiting_queue_size(self):
        return await self.client.llen(_WAITING_QUEUE_KEY)

    async def peek_processing_queue(self, hostname: str) -> str | None:
        res = await self.client.lindex(get_processing_queue_key(hostname), 0)
        if res is None:
            return None
        return res.decode()

    async def get_all_waiting_users(self) -> list[str]:
        raw = await self.client.lrange(_WAITING_QUEUE_KEY, 0, -1)
        return [x.decode() for x in raw]

    async def pop_notification_blocking(self) -> PopNotification | None:
        raw = await self.client.brpop([_NOTIFICATIONS_QUEUE_KEY], 0)
        if raw is None:
            return None
        pop_notification = PopNotification()
        pop_notification.ParseFromString(raw[1])
        return pop_notification

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

    async def get_heartbeat(self, hostname: str) -> HeartbeatData | None:
        raw = await self.client.get(get_host_heartbeat(hostname))
        if raw is None:
            return None
        heartbeat = HeartbeatData()
        heartbeat.ParseFromString(raw)
        return heartbeat

    ####################
    #  HOST FUNCTIONS  #
    ####################

    async def pop_waiting(self, hostname: str) -> str | None:
        res = await self.client.invoke_script(
            _POP_USER_SCRIPT,
            keys=[
                _WAITING_QUEUE_KEY,
                get_processing_queue_key(hostname),
                get_user_assigned_host_key(hostname)
            ],
            args=[hostname]
        )

        if res is None:
            return None

        return res.decode()

    async def flush_processing_queue(self, hostname: str):
        await self.client.delete([get_processing_queue_key(hostname)])

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

        transaction.set(
            get_host_slot_key(hostname),
            notif.SerializeToString()
        )

        transaction.lpush(
            _NOTIFICATIONS_QUEUE_KEY,
            [notif.SerializeToString()]
        )

        transaction.lrem(
            get_processing_queue_key(hostname),
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

    async def send_notification(self, data: PopNotification):
        assert self.client is not None
        return await self.client.lpush(
            _NOTIFICATIONS_QUEUE_KEY,
            [data.SerializeToString()]
        )

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
