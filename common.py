import datetime
from dataclasses import dataclass
from enum import Enum

# seconds
STATUS_UPDATE_INTERVAL = 5

class STATUS(Enum):
    AWAITING = "AWAITING"
    IN_USE = "IN_USE"

@dataclass
class PopNotification:
    hostname: str
    unix_user: str

@dataclass
class HostStatus:
    hostname: str
    status: str
    expiry: datetime.datetime
    current_user: str | None