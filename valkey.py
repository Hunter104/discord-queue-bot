import dataclasses
import datetime
import functools
import json
import os
import logging
from typing import Callable, Awaitable, Any, Concatenate, ParamSpec, TypeVar, List

import msgspec.json
from glide import GlideClientConfiguration, NodeAddress, GlideClient
from glide.glide import Script
from glide_shared import ExpirySet, ExpiryType

import common
from common import PopNotification, HostStatus

logger = logging.getLogger(__name__)

_ADD_USER_SCRIPT = Script("""
    if server.call("lpos", KEYS[1], ARGV[1]) then
        return 1
    end
    server.call("lpush", KEYS[1], ARGV[1])
    return 0
""")

_config: GlideClientConfiguration | None = None

def configure(host, port):
    global _config
    _config = GlideClientConfiguration([NodeAddress(host, port)], request_timeout=500)

P = ParamSpec('P')
R = TypeVar('R')

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

@with_client
async def addUser(client: GlideClient, username: str):
    return await client.invoke_script(_ADD_USER_SCRIPT, ['queue'], [username]) == 0

@with_client
async def popBlocking(client:GlideClient) -> str | None:
    res = await client.brpop(['queue'], 0)
    if res is None:
        return None
    return res[1].decode()

@with_client
async def removeUser(client: GlideClient, user_id: int):
    await client.lrem('queue', 0, str(user_id))

@with_client
async def getSize(client: GlideClient):
    return await client.llen('queue')

# TODO: botar todos esses nomes de chaves em um arquivo de configuração
@with_client
async def getHosts(client: GlideClient) -> List[str]:
    hosts = []
    cursor = '0'
    while True:
        cursor, vals = await client.scan(cursor, 'rpi:*')
        hosts += [val.decode().removeprefix('rpi:') for val in vals]
        if cursor.decode() == '0':
            break
    return hosts

@with_client
async def setHostStatus(client: GlideClient, status: HostStatus):
    return await client.set(f'rpi:{status.hostname}', msgspec.json.encode(status), expiry=ExpirySet(ExpiryType.SEC, common.STATUS_UPDATE_INTERVAL*2))

@with_client
async def getHostStatus(client:GlideClient , name) -> HostStatus | None:
    raw = await client.get(f'rpi:{name}')
    if raw is None:
        return None
    return msgspec.json.decode(raw, type=HostStatus)

@with_client
async def sendNotification(client: GlideClient, data: PopNotification):
    return await client.lpush('notifs', [msgspec.json.encode(data)])

@with_client
async def popNotificationBlocking(client: GlideClient) -> PopNotification | None:
    raw = await client.brpop(['notifs'], 0)
    if raw is None:
        return None
    return msgspec.json.decode(raw[1].decode(), type=PopNotification)

@with_client
async def getAll(client: GlideClient) -> list[str]:
    raw = await client.lrange('queue', 0, -1)
    return [x.decode() for x in raw]