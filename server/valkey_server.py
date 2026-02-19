import datetime
import logging

import glide
from glide import GlideClient
from glide_shared import ExpirySet, ExpiryType, Batch

from common.proto.protocol_pb2 import HostStatus, PopNotification
from common.valkey_defs import *

logger = logging.getLogger(__name__)

async def register_host(client: GlideClient, hostname: str):
    await client.sadd(REGISTERED_HOSTS_KEY, [hostname])

async def pop_waiting_queue_blocking(client: GlideClient) -> str | None:
    res = await client.blmove(WAITING_QUEUE_KEY, PROCESSING_QUEUE_KEY, glide.ListDirection.LEFT,
                              glide.ListDirection.RIGHT, timeout=0)
    if res is None:
        return None
    user = res.decode()
    # TODO: set timeout as parameter
    await client.set(get_user_lock_key(user), "doodoo", expiry=ExpirySet(expiry_type=ExpiryType.SEC, value=5))
    return user


async def finish_processing(client: GlideClient, host_status: HostStatus, timeout: datetime.timedelta):
    if not host_status.HasField("current_user") or not host_status.HasField("expiry"):
        raise ValueError("Host status must have current user and expiry set")

    transaction = Batch(is_atomic=True)

    notif = PopNotification()
    notif.hostname = host_status.hostname
    notif.unix_user = host_status.current_user

    transaction.set(
        get_host_status_key(host_status.hostname),
        host_status.SerializeToString(),
        expiry=ExpirySet(expiry_type=ExpiryType.SEC, value=timeout)
    )

    transaction.set(
        get_user_assigned_host_key(host_status.current_user),
        host_status.hostname,
        expiry=ExpirySet(expiry_type=ExpiryType.SEC, value=timeout)
    )

    transaction.lpush(NOTIFICATIONS_QUEUE_KEY, [notif.SerializeToString()])
    transaction.lrem(PROCESSING_QUEUE_KEY, 0, host_status.hostname)
    await client.exec(transaction, True)


async def release_user(client: GlideClient, hostname: str):
    await client.delete([get_user_assigned_host_key(hostname)])


async def update_status(client: GlideClient, host_data: HostStatus, timeout: datetime.timedelta):
    transaction = Batch(is_atomic=True)
    if host_data.is_occupied:
        transaction.set(get_user_assigned_host_key(host_data.current_user), host_data.hostname,
                        expiry=ExpirySet(expiry_type=ExpiryType.SEC, value=timeout))
    transaction.set(
        get_host_status_key(host_data.hostname),
        host_data.SerializeToString(),
        expiry=ExpirySet(expiry_type=ExpiryType.SEC, value=timeout)
    )
    await client.exec(transaction, True)


# TODO: fazer um wrapper pros protobufs
async def get_host_data(client: GlideClient, hostname: str):
    data = HostStatus()
    raw = await client.get(get_host_status_key(hostname))
    return data.ParseFromString(raw)
