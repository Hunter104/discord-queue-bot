import asyncio
import logging
import os
import socket
from datetime import datetime, timedelta

from glide_shared import NodeAddress

from common import HostStatus, HeartbeatData, PopNotification

from dotenv import load_dotenv
from glide import GlideClientConfiguration

from valkey import ValkeyConnection, get_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


_POLLING_INTERVAL = 5
_HEARTBEAT_TIMEOUT = 10

class HostController:
    def __init__(self, name: str, whitelist_path: str, time_slice: timedelta, valkey_config: GlideClientConfiguration):
        self.name = name
        self.whitelist_path = whitelist_path
        self.time_slice = time_slice

        self.status = HostStatus.AWAITING
        self.current_user: str | None = None
        self.expiry: datetime = datetime.max

        self._expiry_task: asyncio.Task | None = None
        self._shutdown = asyncio.Event()

        self._valkey_config = valkey_config

    def get_whitelist(self) -> list[str]:
        if not os.path.exists(self.whitelist_path):
            return []
        with open(self.whitelist_path, "r") as f:
            return f.read().splitlines()

    def set_whitelist(self, whitelist: list[str]) -> None:
        with open(self.whitelist_path, "w") as f:
            f.write("\n".join(whitelist))

    async def process_user(self, user: str, conn: ValkeyConnection):
        logger.info(f"Processing user {user}...")
        await self._set_user(user, datetime.now() + self.time_slice)
        await conn.finish_processing(self.name, user, expiry=self.expiry)

    async def _set_user(self, user: str, expiry: datetime):
        logger.info(f"Setting user {user} as in use")
        self.current_user = user
        self.status = HostStatus.IN_USE
        self.expiry = expiry

        self.set_whitelist([user])

        self._expiry_task = asyncio.create_task(self._expiry_timer())


    async def release_user(self, conn: ValkeyConnection):
        if not conn:
            raise ValueError("Connection cannot be None")

        if not self.current_user:
            logger.warning("No user to release")
            return

        logger.info(f"Releasing user {self.current_user}")

        self.set_whitelist([])

        # TODO: Remove SSH sessions

        user = self.current_user
        self.current_user = None
        self.status = HostStatus.AWAITING
        self.expiry = datetime.max

        await conn.release_user(user)


    # TODO: maybe this has some synchronization problems with the fetch user loop
    async def _expiry_timer(self):
        try:
            sleep_period = self.expiry - datetime.now()
            if sleep_period <= timedelta(0):
                logger.warning(f"Expiration date {self.expiry} has already been reached for user {self.current_user}")
                async with get_connection(self._valkey_config) as conn:
                    await self.release_user(conn)
            else:
                logger.info(f"User {self.current_user} will expire in {sleep_period}")
                logger.info(f"Sleeping for {sleep_period.total_seconds()} seconds")
                await asyncio.sleep(sleep_period.total_seconds())
                async with get_connection(self._valkey_config) as conn:
                    await self.release_user(conn)
        except asyncio.CancelledError:
            logger.info("Expiry timer cancelled")
            raise

    async def fetch_user_loop(self):
        if self.status == HostStatus.IN_USE:
            logger.info("In use, waiting for session end")
        elif self.status == HostStatus.AWAITING:
            logger.info("Waiting for user...")
        else:
            logger.warning("Unknown state %s", self.status)

        while not self._shutdown.is_set():
            if self.status == HostStatus.AWAITING:
                async with get_connection(self._valkey_config) as conn:
                    user = await conn.pop_waiting(self.name)
                    if user:
                        await self.process_user(user, conn)

            await asyncio.sleep(_POLLING_INTERVAL)

    async def send_heartbeat_loop(self):
        logger.info("Starting heartbeat loop")
        while not self._shutdown.is_set():
            async with get_connection(self._valkey_config) as conn:
                await conn.send_heartbeat(
                                            HeartbeatData(
                                                self.name,
                                                self.status,
                                                self.expiry,
                                                self.current_user,
                                                datetime.now()
                                            ),
                                            timedelta(seconds=_HEARTBEAT_TIMEOUT))
                await asyncio.sleep(_HEARTBEAT_TIMEOUT/2)


    async def shutdown(self):
        logger.info("Shutting down host controller...")
        self._shutdown.set()

        if self._expiry_task:
            self._expiry_task.cancel()
            try:
                await self._expiry_task
            except asyncio.CancelledError:
                pass

    async def run(self):
        logger.info("Starting host controller...")
        try:
            async with get_connection(self._valkey_config) as conn:
                last_user = await conn.peek_processing_queue(self.name)
                slot = await conn.get_slot(self.name)

                # If there is a slot, then processing has already finished
                if slot:
                    logger.info("Slot found for user %s til %s, recovering...", slot.current_user, slot.expiry)
                    # Recover the last user if the server stopped before finishing processing
                    await self._set_user(slot.current_user, slot.expiry)
                    # Flush processing queue just in case
                    await conn.flush_processing_queue(self.name)
                elif last_user:
                    logger.info("Found user %s in processing queue, finishing processing...", last_user)
                    # Recover the current slot if the server stopped while serving a user
                    await self.process_user(last_user, conn)


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
        seconds=int(os.getenv("TIME_SLICE", 10*60))
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
        valkey_config=config
    )

    controller.set_whitelist([])

    await controller.run()


if __name__ == "__main__":
    asyncio.run(main())
