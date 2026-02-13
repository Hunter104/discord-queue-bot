import asyncio
import logging
import os
import socket
from datetime import datetime, timedelta
from common import STATUS
from enum import Enum

from discord.ui import user_select
from dotenv import load_dotenv
from numpy.f2py.crackfortran import usermodules

import valkey


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



class HostController:
    def __init__(self, name: str, whitelist_path: str, valkey_client, time_slice: timedelta):
        self.name = name
        self.whitelist_path = whitelist_path
        self.valkey = valkey_client
        self.time_slice = time_slice

        self.status = STATUS.AWAITING
        self.current_user: str | None = None
        self.expiry: datetime = datetime.max

        self._expiry_task: asyncio.Task | None = None
        self._shutdown = asyncio.Event()

    def get_whitelist(self) -> list[str]:
        if not os.path.exists(self.whitelist_path):
            return []
        with open(self.whitelist_path, "r") as f:
            return f.read().splitlines()

    def set_whitelist(self, whitelist: list[str]) -> None:
        with open(self.whitelist_path, "w") as f:
            f.write("\n".join(whitelist))

    async def assign_user(self, user: str):
        logger.info(f"Assigning user {user}")

        self.current_user = user
        self.status = STATUS.IN_USE
        self.expiry = datetime.now() + self.time_slice

        await self.valkey.sendNotification(
            {"name": self.name, "user": user}
        )

        self.set_whitelist([user])

        self._expiry_task = asyncio.create_task(self._expiry_timer())

    async def release_user(self):
        if not self.current_user:
            return

        logger.info(f"Releasing user {self.current_user}")

        self.set_whitelist([])

        # TODO: Remove SSH sessions

        self.current_user = None
        self.status = STATUS.AWAITING
        self.expiry = datetime.max

    async def _expiry_timer(self):
        try:
            await asyncio.sleep(self.time_slice.total_seconds())
            await self.release_user()
        except asyncio.CancelledError:
            logger.info("Expiry timer cancelled")
            raise

    async def fetch_user_loop(self):
        while not self._shutdown.is_set():
            if self.status == STATUS.AWAITING:
                logger.info("Waiting for user...")

                user = await self.valkey.popBlocking()
                if user:
                    await self.assign_user(user.decode())

            await asyncio.sleep(1)

    async def update_status_loop(self):
        while not self._shutdown.is_set():
            await self.valkey.setHostStatus(
                self.name,
                {
                    "status": self.status.value,
                    "expiry": str(self.expiry),
                    "current_user": str(self.current_user),
                },
            )
            await asyncio.sleep(5)

    async def recover_last_state(self):
        last_state = await self.valkey.getHostStatus(self.name)
        if last_state:
            self.status = STATUS(last_state["status"])
            self.expiry = datetime.fromisoformat(last_state["expiry"])
            self.current_user = last_state["current_user"]

    async def shutdown(self):
        logger.info("Shutting down host controller...")
        self._shutdown.set()

        if self._expiry_task:
            self._expiry_task.cancel()
            try:
                await self._expiry_task
            except asyncio.CancelledError:
                pass

        await self.release_user()

    async def run(self):
        try:
            await asyncio.gather(
                self.fetch_user_loop(),
                self.update_status_loop(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()


async def main():
    load_dotenv()

    time_slice = timedelta(
        seconds=int(os.getenv("TIME_SLICE", 600))
    )

    name = socket.gethostname()
    whitelist_path = os.environ["WHITELIST_PATH"]

    valkey.configure(
        os.environ["VALKEY_HOST"],
        int(os.environ["VALKEY_PORT"]),
    )

    controller = HostController(
        name=name,
        whitelist_path=whitelist_path,
        valkey_client=valkey,
        time_slice=time_slice,
    )

    controller.set_whitelist([])

    await controller.run()


if __name__ == "__main__":
    asyncio.run(main())
