import enum
from typing import Any

# ----- Base classes -----


class MessageType(enum.Enum):
    ClientRequest = 0
    RequestVote = 1
    RequestVoteResponse = 2
    Acknowledgement = 3
    AppendEntries = 4


class LogEntry:
    def __init__(self, term: int, key: Any, value: Any | None):
        self.term = term
        self.key = key
        self.value = value  # None means that key should be deleted

    def to_dict(self) -> dict:
        return {
            'term': self.term,
            'key': self.key,
            'value': self.value
        }


class BaseMessage:
    def __init__(self, msg_type: MessageType, data: dict | None):
        self.type: MessageType = msg_type
        if data is not None:
            self.term: int = data['term']
            self.sender_id: int = data['sender_id']

    def to_dict(self) -> dict:
        return {
            'type': self.type.name,
            'term': self.term,
            'sender_id': self.sender_id
        }

# ----- Message implementation -----


class RequestVote(BaseMessage):
    def __init__(self, data: dict):
        super().__init__(MessageType.RequestVote, data)
        self.log_term: int = data['log_term']
        self.log_length: int = data['log_length']

    def to_dict(self) -> dict:
        result = super().to_dict()
        result.update({
            'log_term': self.log_term,
            'log_length': self.log_length
        })
        return result


class RequestVoteResponse(BaseMessage):
    def __init__(self, data: dict):
        super().__init__(MessageType.RequestVoteResponse, data)
        self.voted: bool = data['voted']

    def to_dict(self) -> dict:
        result = super().to_dict()
        result.update({
            'voted': self.voted
        })
        return result


class AppendEntries(BaseMessage):
    def __init__(self, data: dict):
        super().__init__(MessageType.AppendEntries, data)
        self.log_term: int = data['log_term']
        self.log_length: int = data['log_length']
        self.committed: int = data['committed']
        self.entries: list[LogEntry] = [
            entry if isinstance(entry, LogEntry) else LogEntry(**entry)
            for entry in data.get('entries', [])
        ]

    def to_dict(self) -> dict:
        result = super().to_dict()
        result.update({
            'log_term': self.log_term,
            'log_length': self.log_length,
            'committed': self.committed,
            'entries': [entry.to_dict() for entry in self.entries]
        })
        return result


class Acknowledgement(BaseMessage):
    def __init__(self, data: dict):
        super().__init__(MessageType.Acknowledgement, data)
        self.ack: bool = data['ack']
        self.acked: int = data['acked']

    def to_dict(self) -> dict:
        result = super().to_dict()
        result.update({
            'ack': self.ack,
            'acked': self.acked
        })
        return result


class ClientRequest(BaseMessage):
    def __init__(self, data: dict):
        super().__init__(MessageType.ClientRequest, None)
        self.key: Any = data['key']
        self.value: Any | None = data.get('value')
        self.operation: str = data['operation']  # e.g., "get", "store", "delete", "inc"
        self.address: str = data['address']  # address to send to

    def to_dict(self) -> dict:
        result = super().to_dict()
        result.update({
            'key': self.key,
            'value': self.value,
            'operation': self.operation,
            'address': self.address
        })
        return result

# ----- Message builder -----


def build_message(msg: dict) -> BaseMessage:
    if 'type' not in msg:
        return ClientRequest(msg)

    msg_type = MessageType[msg['type']]
    if msg_type == MessageType.RequestVote:
        return RequestVote(msg)
    elif msg_type == MessageType.RequestVoteResponse:
        return RequestVoteResponse(msg)
    elif msg_type == MessageType.AppendEntries:
        return AppendEntries(msg)
    elif msg_type == MessageType.Acknowledgement:
        return Acknowledgement(msg)
    return None
