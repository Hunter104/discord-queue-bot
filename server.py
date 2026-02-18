import asyncio
import logging
import os
import socket
import subprocess
from datetime import datetime, timedelta

from glide_shared import NodeAddress

from dotenv import load_dotenv
from glide import GlideClientConfiguration, GlideClient

import protocol_pb2
import valkey

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

_POLLING_INTERVAL = 5
_HEARTBEAT_TIMEOUT = 10


class HostController:
    def __init__(self, name: str, whitelist_path: str, time_slice: timedelta, valkey_client: GlideClient):
        self.name = name
        self.whitelist_path = whitelist_path
        self.time_slice = time_slice

        self.status = protocol_pb2.HostStatus.AWAITING
        self.current_user: str | None = None
        self.expiry: datetime | None = None

        self._shutdown = asyncio.Event()

        self._valkey_client = valkey_client

    def get_whitelist(self) -> list[str]:
        if not os.path.exists(self.whitelist_path):
            return []
        with open(self.whitelist_path, "r") as f:
            return f.read().splitlines()

    def set_whitelist(self, whitelist: list[str]) -> None:
        with open(self.whitelist_path, "w") as f:
            f.write("\n".join(whitelist))

    async def process_user(self, user: str):
        logger.info(f"Processing user {user}...")
        self._set_user(user, datetime.now() + self.time_slice)
        await valkey.finish_processing(self._valkey_client, self.name, user, expiry=self.expiry)

    def _set_user(self, user: str, expiry: datetime):
        logger.info(f"Setting user {user} as in use")
        self.current_user = user
        self.status = protocol_pb2.HostStatus.IN_USE
        self.expiry = expiry
        self.set_whitelist([user])


    async def release_user(self):
        if not self.current_user:
            logger.warning("No user to release")
            return
        subprocess.run(["pkill", "-SIGTERM", "-u", self.current_user], check=True, capture_output=True)

        logger.info(f"Releasing user {self.current_user}")
        self.set_whitelist([])
        user = self.current_user
        self.current_user = None
        self.status = protocol_pb2.HostStatus.AWAITING
        self.expiry = datetime.max

        await valkey.release_user(self._valkey_client, user)

    async def expiry_timer(self):
        try:
            sleep_period = self.expiry - datetime.now()
            if sleep_period <= timedelta(0):
                logger.warning(f"Expiration date {self.expiry} has already been reached for user {self.current_user}")
            else:
                logger.info(f"User %s will expire in %s, sleeping for %.2f seconds", self.current_user, sleep_period, sleep_period.total_seconds())
                await asyncio.sleep(sleep_period.total_seconds())
                await valkey.release_user(self._valkey_client, self.name)
        except asyncio.CancelledError:
            logger.info("Expiry timer cancelled")
            raise

    async def fetch_user_loop(self):
        slot = await valkey.get_slot(self._valkey_client, self.name)
        if slot:
            logger.info("Slot found for user %s til %s, recovering...", slot.current_user, slot.expiry)
            self._set_user(slot.current_user, slot.expiry.ToDatetime())
            await self.expiry_timer()

        while not self._shutdown.is_set():
            user = await valkey.pop_waiting_queue_blocking(self._valkey_client)
            if user:
                await self.process_user(user)
                await self.expiry_timer()

    async def send_heartbeat_loop(self):
        logger.info("Starting heartbeat loop")
        while not self._shutdown.is_set():
                await valkey.send_heartbeat(
                    self._valkey_client,
                    self.name,
                    self.status,
                    self.expiry,
                    self.current_user,
                    datetime.now(),
                    timedelta(seconds=_HEARTBEAT_TIMEOUT),
                )
                await asyncio.sleep(_HEARTBEAT_TIMEOUT / 2)

    async def shutdown(self):
        logger.info("Shutting down host controller...")
        self._shutdown.set()

    async def run(self):
        logger.info("Starting host controller...")
        try:
            await asyncio.gather(
                self.fetch_user_loop(),
                self.send_heartbeat_loop(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()


async def main():
    load_dotenv()

    time_slice = timedelta(
        seconds=int(os.getenv("TIME_SLICE", 10 * 60))
    )
    logger.info(f"Configuring time slice for %s", time_slice)
    logger.info("Polling interval: %s seconds", _POLLING_INTERVAL)
    logger.info("Timeout for heartbeats: %s seconds", _HEARTBEAT_TIMEOUT)

    name = socket.gethostname()
    logger.info("Starting host controller for hostname: %s", name)

    whitelist_path = os.environ["WHITELIST_PATH"]
    logger.info(f"Using whitelist path: %s", whitelist_path)

    config = GlideClientConfiguration(
        [NodeAddress(
            os.environ["VALKEY_HOST"],
            int(os.environ["VALKEY_PORT"]),
        )]
    )

    logger.info("Starting Valkey configuration for: %s:%s", os.environ["VALKEY_HOST"], os.environ["VALKEY_PORT"])

    controller = HostController(
        name=name,
        whitelist_path=whitelist_path,
        time_slice=time_slice,
        valkey_client=await GlideClient.create(config),
    )

    controller.set_whitelist([])

    await controller.run()


if __name__ == "__main__":
    asyncio.run(main())
