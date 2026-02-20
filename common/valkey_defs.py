STATUS_MESSAGE_LIST_KEY = 'status_messages'
NOTIFICATIONS_QUEUE_KEY = 'notifs'
WAITING_QUEUE_KEY = 'waiting_queue'
PROCESSING_QUEUE_KEY = 'processing_queue'
REGISTERED_HOSTS_KEY = 'hosts'

LOCK_PREFIX = 'lock:'

HOST_KEY_PREFIX = 'rpi'
HOST_STATUS_SUFFIX = 'status'

USER_KEY_PREFIX = 'user'
USER_ASSIGNMENT_KEY_SUFFIX = 'assigned_to'
USER_DISCORD_ID_SUFFIX = 'discord_id'
USER_UNIX_ID_SUFFIX = 'unix_id'

DISCORD_CHANNEL_PREFIX = 'channel'
STATUS_MESSAGE_CHANNEL_LIST_KEY = 'messages'
STATUS_MESSAGE_CHANNELS_KEY = 'status_message_channels'


def get_user_lock_key(username: str) -> str:
    return f'{LOCK_PREFIX}:{username}'


def get_status_message_channels_key() -> str:
    return f'{STATUS_MESSAGE_LIST_KEY}:{STATUS_MESSAGE_CHANNELS_KEY}'


def get_status_message_channel_list_key(channel_id: int) -> str:
    return f'{DISCORD_CHANNEL_PREFIX}:{channel_id}:{STATUS_MESSAGE_CHANNEL_LIST_KEY}'


def get_user_discord_id_key(username: str) -> str:
    return f'{USER_KEY_PREFIX}:{username}:{USER_DISCORD_ID_SUFFIX}'


def get_user_unix_id_key(discord_id: str) -> str:
    return f'{USER_KEY_PREFIX}:{discord_id}:{USER_UNIX_ID_SUFFIX}'


def get_user_assigned_to_host_key(username: str) -> str:
    return f'{USER_KEY_PREFIX}:{username}:{USER_ASSIGNMENT_KEY_SUFFIX}'


def get_host_status_key(hostname: str):
    return f'{HOST_KEY_PREFIX}:{hostname}:{HOST_STATUS_SUFFIX}'


