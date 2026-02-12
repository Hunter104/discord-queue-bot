import asyncio
import logging
import os
import socket
from datetime import datetime, timedelta
from enum import Enum

from dotenv import load_dotenv
from glide import Script

import valkey

logger = logging.getLogger(__name__)

#Default is 10 minutes
if "TIME_SLICE" not in os.environ:
    TIME_SLICE = timedelta(minutes=10)
else:
    TIME_SLICE = timedelta(seconds=int(os.environ["TIME_SLICE"]))

class STATUS(Enum):
    AWAITING = "AWAITING"
    IN_USE = "IN_USE"

name = None
status = None
current_user = None
expiry = None
whitelist_path = None

def getWhitelist():
    with open(whitelist_path, 'r') as f:
        return f.readlines()

def setWhitelist(whitelist):
    with open(whitelist_path, 'w') as f:
        f.write(('\n'.join(whitelist)).strip())

async def fetchUser():
    global status
    global expiry
    global current_user
    while True:
        logger.info('Fetching user...')
        if status == STATUS.AWAITING:
            logger.info('Awaiting user...')
            current_user = (await valkey.popBlocking()).decode()
            if current_user is not None:
                logger.info(f'Assigned user {current_user}')
                # Notify the user that has been assigned
                await valkey.sendNotification({'name': name, 'user_id': current_user},)
                setWhitelist([current_user])
                expiry = datetime.now() + TIME_SLICE
                status = STATUS.IN_USE
                logger.info(f'User {current_user} is now in use, waiting...')
                await asyncio.sleep(TIME_SLICE.total_seconds())
                logger.info(f'End of time slice, clearing user {current_user}...')
                setWhitelist([])
                # Remove all user ssh sessions

async def updateStatus():
    while True:
        logger.info('Updating status...')
        await valkey.setHostStatus(name, {'status': status.value, 'expiry': str(expiry), 'current_user': str(current_user)})
        await asyncio.sleep(5)

async def main():
    logger.info('Starting server...')
    load_dotenv()
    global name
    name = socket.gethostname()
    global status
    status = STATUS.AWAITING
    global current_user
    current_user = None
    global expiry
    expiry = datetime.max
    global whitelist_path
    whitelist_path = os.environ["WHITELIST_PATH"]
    valkey.configure(os.environ["VALKEY_HOST"], int(os.environ["VALKEY_PORT"]))
    setWhitelist([])
    await asyncio.gather(
        updateStatus(),
        fetchUser(),
    )

asyncio.run(main())