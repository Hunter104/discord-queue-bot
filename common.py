from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import msgspec

# seconds
STATUS_UPDATE_INTERVAL = 5

class STATUS(Enum):
    AWAITING = "AWAITING"
    IN_USE = "IN_USE"

class PopNotification(msgspec.Struct):
    hostname: str
    unix_user: str

class HostStatus(msgspec.Struct):
    hostname: str
    status: STATUS
    expiry: datetime
    current_user: str | None