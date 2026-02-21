import asyncio
import logging
import os
import socket
import subprocess
from datetime import datetime, timedelta

from dotenv import load_dotenv
from glide import GlideClientConfiguration, GlideClient
from glide_shared import NodeAddress

import valkey_server as valkey
from valkey_server import HostStatus

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class HostController:
    def __init__(self, whitelist_path: str, valkey_host: str = "127.0.0.1", valkey_port: int = 6379,
                 host_name: str = socket.gethostname(), heartbeat_timeout: timedelta = timedelta(minutes=1),
                 heartbeat_update: float = 2, time_slice: timedelta = timedelta(seconds=10 * 60)):
        self.whitelist_path: str = whitelist_path
        self.time_slice: timedelta = time_slice
        self.current_status: HostStatus = HostStatus(hostname=host_name, is_occupied=False, expiry=None, current_user=None,
                                                     last_timestamp=datetime.now())
        self.valkey_host: str = valkey_host
        self.valkey_port: int = valkey_port
        self.config: GlideClientConfiguration = GlideClientConfiguration([NodeAddress(valkey_host, valkey_port)])
        self.heartbeat_timeout: timedelta = heartbeat_timeout
        self.heartbeat_update: float = heartbeat_update

    def __str__(self):
        return f"HostController(whitelist_path={self.whitelist_path}, host_name={self.current_status.hostname}, " \
               f"heartbeat_timeout={self.heartbeat_timeout}, heartbeat_update={self.heartbeat_update}, " \
               f"time_slice={self.time_slice}, valkey_address={self.valkey_host}:{self.valkey_port})"

    def set_whitelist(self, whitelist: list[str]) -> None:
        with open(self.whitelist_path, "w") as f:
            f.write("\n".join(whitelist))

    async def expiry_timer(self, client: GlideClient):
        sleep_period = self.current_status.expiry - datetime.now()
        if sleep_period <= timedelta(0):
            logger.warning("User %s has already expired", self.current_status.current_user)
            return
        logger.info("User %s will expire in %s", self.current_status.current_user, self.current_status.expiry)
        await asyncio.sleep(sleep_period.total_seconds())
        logger.info("Releasing user %s", self.current_status.current_user)
        subprocess.run(["pkill", "-SIGTERM", "-u", self.current_status.current_user], check=True, capture_output=True)
        await valkey.release_user(client, self.current_status.current_user)
        self.current_status.is_occupied = False
        self.current_status.current_user = None
        self.current_status.expiry = None
        self.set_whitelist([])

    async def fetch_user_loop(self):
        client = await GlideClient.create(self.config)
        if last_status := await valkey.get_host_data(client, self.current_status.hostname):
            logger.info("Recovering last status %s", last_status)
            self.current_status = last_status
        while True:
            if not self.current_status.is_occupied:
                user = await valkey.pop_waiting_queue_blocking(client)
                logger.info("Found user %s", user)
                expiry = datetime.now() + self.time_slice
                self.current_status.is_occupied = True
                self.current_status.current_user = user
                self.current_status.expiry = expiry
                self.set_whitelist([user])
                await valkey.finish_processing(client, self.current_status, self.heartbeat_timeout)
            await self.expiry_timer(client)

    async def send_heartbeat_loop(self):
        client = await GlideClient.create(self.config)
        logger.info("Starting heartbeat loop")
        while True:
            self.current_status.last_heartbeat = datetime.now()
            await valkey.update_status(client, self.current_status, self.heartbeat_timeout)
            await asyncio.sleep(self.heartbeat_update)

    async def run(self):
        client = await GlideClient.create(self.config)
        logger.info("Starting queue daemon...")
        self.set_whitelist([])
        await valkey.register_host(client, self.current_status.hostname)
        await client.close()
        await asyncio.gather(self.fetch_user_loop(), self.send_heartbeat_loop())

async def main():
    load_dotenv()
    controller = HostController(
        whitelist_path=os.environ["WHITELIST_PATH"],
        valkey_host=os.environ["VALKEY_HOST"],
        valkey_port=int(os.environ["VALKEY_PORT"]),
        time_slice=timedelta(
            seconds=int(os.getenv("TIME_SLICE", 600))
        ),
    )
    logger.info("Host configuration: %s", controller)
    await controller.run()

if __name__ == "__main__":
    asyncio.run(main())