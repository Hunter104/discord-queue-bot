from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import msgspec

# seconds
STATUS_UPDATE_INTERVAL = 5

class HostStatus(Enum):
    AWAITING = "AWAITING"
    IN_USE = "IN_USE"

class PopNotification(msgspec.Struct):
    hostname: str
    unix_user: str

class SlotData(msgspec.Struct):
    current_user: str
    expiry: datetime

# TODO: remove redundant data
class HeartbeatData(msgspec.Struct):
    hostname: str
    status: HostStatus
    expiry: datetime | None
    current_user: str | None
    timestamp: datetime