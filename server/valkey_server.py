import logging
from datetime import timedelta

from glide import GlideClient
from glide import ListDirection
from glide_shared import ExpirySet, ExpiryType, Batch

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

async def finish_processing(
        client: GlideClient, host_status: HostStatus, timeout: timedelta
):
    if not host_status.current_user:
        raise ValueError("Host status must have current_user set")

    timeout_secs = int(timeout.total_seconds())
    expiry = ExpirySet(expiry_type=ExpiryType.SEC, value=timeout_secs)

    transaction = Batch(is_atomic=True)

    transaction.set(
        get_user_assigned_to_host_key(host_status.current_user),
        host_status.hostname,
        expiry=expiry
    )
    transaction.hset(
        get_host_status_key(host_status.hostname),
        host_status.to_dict(),
    )
    transaction.expire(get_host_status_key(host_status.hostname), timeout_secs)
    transaction.lrem(PROCESSING_QUEUE_KEY, 0, host_status.current_user)
    transaction.publish(host_status.current_user, get_host_pubsub_channel(host_status.hostname))

    await client.exec(transaction, True)

async def release_user(client: GlideClient, hostname: str):
    await client.delete([get_user_assigned_to_host_key(hostname)])

async def update_status(
        client: GlideClient, host_status: HostStatus, timeout: timedelta
):
    timeout_secs = int(timeout.total_seconds())
    expiry = ExpirySet(expiry_type=ExpiryType.SEC, value=timeout_secs)

    transaction = Batch(is_atomic=True)

    if host_status.is_occupied:
        transaction.set(
            get_user_assigned_to_host_key(host_status.current_user),
            host_status.hostname,
            expiry=expiry
        )
    transaction.hset(
        get_host_status_key(host_status.hostname),
        host_status.to_dict(),
    )
    transaction.expire(get_host_status_key(host_status.hostname), timeout_secs)

    await client.exec(transaction, True)

async def get_host_data(client: GlideClient, hostname: str) -> HostStatus | None:
    raw = await client.hgetall(get_host_status_key(hostname))
    if not raw:
        return None
    return HostStatus.from_hgetall(raw)