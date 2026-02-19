import asyncio
import logging
import os
import socket
import subprocess
from datetime import datetime, timedelta

from glide_shared import NodeAddress

from dotenv import load_dotenv
from glide import GlideClientConfiguration, GlideClient

from common.proto.protocol_pb2 import HostStatus
import valkey_server as valkey

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

_HEARTBEAT_TIMEOUT = timedelta(minutes=1)
_HEARTBEAT_UPDATE = 2


class HostController:
    def __init__(self, name: str, whitelist_path: str, time_slice: timedelta, config: GlideClientConfiguration):
        self.whitelist_path = whitelist_path
        self.time_slice = time_slice
        self.current_status: HostStatus = HostStatus()
        self.current_status.hostname = name
        self.current_status.is_occupied = False
        self.config = config

    def set_whitelist(self, whitelist: list[str]) -> None:
        with open(self.whitelist_path, "w") as f:
            f.write("\n".join(whitelist))

    def _set_user(self, user: str, expiry: datetime):
        logger.info(f"Setting user {user} as in use")
        self.current_status.is_occupied = True
        self.current_status.current_user = user
        self.current_status.expiry.FromDatetime(expiry)
        self.set_whitelist([user])

    def _unset_user(self) -> str:
        logger.info(f"Unsetting user {self.current_status.current_user}")
        user = self.current_status.current_user
        self.current_status.is_occupied = False
        self.current_status.ClearField("current_user")
        self.current_status.ClearField("expiry")
        self.set_whitelist([])
        return user

    async def release_user(self, client: GlideClient):
        if not self.current_status.is_occupied:
            logger.warning("No user to release")
            return
        subprocess.run(["pkill", "-SIGTERM", "-u", self.current_status.current_user], check=True, capture_output=True)
        user = self._unset_user()
        await valkey.release_user(client, user)

    async def expiry_timer(self, client: GlideClient):
        sleep_period = self.current_status.expiry.ToDatetime() - datetime.now()
        if sleep_period <= timedelta(0):
            logger.warning(
                f"Expiration date {self.current_status.current_user} has already been reached for user {self.current_status.current_user}")
        else:
            logger.info(f"User %s will expire in %s, sleeping for %.2f seconds", self.current_status.current_user,
                        sleep_period, sleep_period.total_seconds())
            await asyncio.sleep(sleep_period.total_seconds())
            await self.release_user(client)

    async def fetch_user_loop(self):
        client = await GlideClient.create(self.config)
        last_status = await valkey.get_host_data(client, self.current_status.hostname)
        if last_status:
            logger.info("Recovering last status %s", last_status)
            self.current_status = last_status
        while True:
            if not self.current_status.is_occupied:
                user = await valkey.pop_waiting_queue_blocking(client)
                if user:
                    logger.info("Found user %s", user)
                    self._set_user(user, datetime.now() + self.time_slice)
                    await valkey.finish_processing(client, self.current_status, _HEARTBEAT_TIMEOUT)
            await self.expiry_timer(client)

    async def send_heartbeat_loop(self):
        client = await GlideClient.create(self.config)
        logger.info("Starting heartbeat loop")
        while True:
            self.current_status.last_heartbeat.FromDatetime(datetime.now())
            await valkey.update_status(client, self.current_status, _HEARTBEAT_TIMEOUT)
            await asyncio.sleep(_HEARTBEAT_UPDATE)

    async def run(self):
        client = await GlideClient.create(self.config)
        logger.info("Starting queue daemon...")
        self.set_whitelist([])
        await valkey.register_host(client, self.current_status.hostname)
        await client.close()
        await asyncio.gather(self.fetch_user_loop(), self.send_heartbeat_loop())


async def main():
    load_dotenv()

    time_slice = timedelta(
        seconds=int(os.getenv("TIME_SLICE", 10 * 60))
    )
    logger.info(f"Configuring time slice for %s", time_slice)
    logger.info("Timeout for heartbeats: %s", _HEARTBEAT_TIMEOUT)
    logger.info("Heartbeat update interval: %s", _HEARTBEAT_UPDATE)

    name = socket.gethostname()
    logger.info("Starting host controller for hostname: %s", name)

    whitelist_path = os.environ["WHITELIST_PATH"]
    logger.info(f"Using whitelist path: %s", whitelist_path)

    logger.info("Starting Valkey client for: %s:%s", os.environ["VALKEY_HOST"], os.environ["VALKEY_PORT"])
    config = GlideClientConfiguration([NodeAddress(os.environ["VALKEY_HOST"], int(os.environ["VALKEY_PORT"]), )])

    controller = HostController(
        name=name,
        whitelist_path=whitelist_path,
        time_slice=time_slice,
        config=config,
    )

    await controller.run()


if __name__ == "__main__":
    asyncio.run(main())
