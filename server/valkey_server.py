import datetime
import logging
from datetime import timedelta
from tkinter.constants import LEFT

import glide
from glide import GlideClient
from glide_shared import ExpirySet, ExpiryType, Batch
from glide import ListDirection

from common.proto.protocol_pb2 import HostStatus, PopNotification
from common.valkey_defs import *

logger = logging.getLogger(__name__)

async def register_host(client: GlideClient, hostname: str):
    await client.sadd(REGISTERED_HOSTS_KEY, [hostname])

async def pop_waiting_queue_blocking(client: GlideClient, lock_timeout: timedelta = timedelta(seconds=5)) -> str:
    res = await client.blmove(WAITING_QUEUE_KEY, PROCESSING_QUEUE_KEY, ListDirection.LEFT,
                              ListDirection.RIGHT, timeout=0)
    if res is None:
        raise RuntimeError("Null reply from blmove")
    user = res.decode()
    await client.set(get_user_lock_key(user), "doodoo",
                     expiry=ExpirySet(expiry_type=ExpiryType.SEC, value=lock_timeout))
    return user


async def finish_processing(client: GlideClient, host_status: HostStatus, timeout: datetime.timedelta):
    if not host_status.HasField("current_user") or not host_status.HasField("expiry"):
        raise ValueError("Host status must have current user and expiry set")

    expiry = ExpirySet(expiry_type=ExpiryType.SEC, value=timeout)
    transaction = Batch(is_atomic=True)

    transaction.set(get_host_status_key(host_status.hostname), host_status.SerializeToString(), expiry=expiry)
    transaction.set(get_user_assigned_to_host_key(host_status.current_user), host_status.hostname, expiry=expiry)

    notif = PopNotification(hostname=host_status.hostname, unix_user=host_status.current_user)
    transaction.lpush(NOTIFICATIONS_QUEUE_KEY, [notif.SerializeToString()])
    transaction.lrem(PROCESSING_QUEUE_KEY, 0, host_status.hostname)
    await client.exec(transaction, True)


async def release_user(client: GlideClient, hostname: str):
    await client.delete([get_user_assigned_to_host_key(hostname)])


async def update_status(client: GlideClient, host_data: HostStatus, timeout: datetime.timedelta):
    expiry = ExpirySet(expiry_type=ExpiryType.SEC, value=timeout)
    transaction = Batch(is_atomic=True)
    if host_data.is_occupied:
        transaction.set(get_user_assigned_to_host_key(host_data.current_user), host_data.hostname, expiry=expiry)
    transaction.set(get_host_status_key(host_data.hostname), host_data.SerializeToString(), expiry=expiry)
    await client.exec(transaction, True)


# TODO: fazer um wrapper pros protobufs
async def get_host_data(client: GlideClient, hostname: str):
    raw = await client.get(get_host_status_key(hostname))
    if raw is None:
        return None
    data = HostStatus()
    data.ParseFromString(raw)
    return data
