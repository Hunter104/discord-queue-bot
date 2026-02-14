from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import msgspec

# seconds
STATUS_UPDATE_INTERVAL = 5

class PopNotification(msgspec.Struct):
    hostname: str
    unix_user: str

class SlotData(msgspec.Struct):
    current_user: str
    expiry: datetime