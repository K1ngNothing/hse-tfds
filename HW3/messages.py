# Message fields
KEY = 'key'
MESSAGE = 'message'
MESSAGE_ID = 'message_id'
MESSAGE_TYPE = 'type'
OPERATIONS = 'operations'
SENDER_ID = 'sender_id'
TIMEOUT = 'timeout'
TIMESTAMP = 'timestamp'
VALUE = 'value'

# Message types
CLIENT_REQUEST = 'client_request'
SLEEP = 'sleep'

def get_message_unique_id(msg: dict) -> str:
    return f"{msg[SENDER_ID]}, {msg[MESSAGE_ID]}"
