import time
import json
import random
import threading
import enum
from queue import Queue, Empty
from typing import cast

from .communications import Listener, Talker
from .protocol import ClientRequest, MessageType, BaseMessage, RequestVote, RequestVoteResponse, \
    Acknowledgement, AppendEntries, CommitEntries, LogEntry


class Role(enum.Enum):
    Leader = 0,
    Follower = 1,
    Candidate = 2,
    Disabled = 3,


def get_address(id: int) -> str:
    local_ip = '127.0.0.1'
    starting_port = 8000
    return local_ip + ':' + str(starting_port + id)


class RaftNode(threading.Thread):
    def __init__(self, id: int, node_count: int, role: Role = Role.Follower):
        super().__init__(self)
        self.id = id
        self.node_count: int = node_count
        self._data: dict[any, any] = {}

        # Basic state variables
        self._role: Role = role
        self._leader_id: int = None
        self._term: int = 1

        # Log variables
        self._log: list[LogEntry] = []
        self._committed: int = 0
        self._log_term: int = 0

        # Voting variables
        self._voted_for: int | None = None
        self._votes_received: set[int] = set()

        # Other nodes state
        self._to_send = [0 for _ in range(self.node_count)]
        self._acked_length = [0 for _ in range(self.node_count)]

        # Outside manipulation
        self._stopped = threading.Event()

        # Timing variables
        self._election_timeout = random.uniform(0.1, 0.2)
        self._heartbeat_delta = 0.01

    def stop(self):
        self._stopped.set()
        print(f'Noe {self.id} is stopped')

    def pause(self):
        self._role = Role.Disabled
        print(f'Node {self.id} is paused')

    def unpause(self):
        if self._role == Role.Disabled:
            self._role = Role.Follower
        print(f'Node {self.id} is unpaused')

    def run(self):
        while not self._stopped.is_set():
            if self._role == Role.Leader:
                self._leader_flow()
            elif self._role == Role.Follower:
                self._follower_flow()
            elif self._role == Role.Candidate:
                self._candidate_flow()
            else:
                # Node is disabled. Just chill a bit.
                time.sleep(1)

    # ----- Flows for each role -----

    def _follower_flow(self) -> None:
        last_heartbeat = time.time()

        while not self._stopped.is_set() and self._role == Role.Candidate:
            message = self._get_message()

            if message.type == MessageType.RequestVote:
                # Try to vote for candidate
                self._check_and_vote(message)

                # Vote in progress => we should not promote to Candidate
                last_heartbeat = time.time()

            elif message.type == MessageType.AppendEntries:
                if message.term > self._term:
                    # We're old => update term
                    self._term = message.term
                    self.voted_for = None
                    self.leader_id = message.sender_id

                # Try to append entries
                self._check_and_append_entries(message)

                # We've got a message from leader => update timer
                last_heartbeat = time.time()

            elif message.type == MessageType.ClientRequest:
                self._send_client_to_leader()

            # If you haven't heard a heartbeat in a while, promote yourself to a candidate
            if time.time() - last_heartbeat > self._election_timeout:
                self._role = Role.Candidate
                return

    def _candidate_flow(self):
        self._term += 1
        self._voted_for = self.id
        self._votes_received = set(self.id)
        self._broadcast_vote_request()

        election_started = time.time()

        while not self._stopped.is_set() and self._role == Role.Candidate:
            message = self._get_message()

            if message.type == MessageType.RequestVoteResponse:
                message = cast(RequestVoteResponse, message)

                if message.term > self._term:
                    # Someone has a higher term => become follower
                    self._term = message.term
                    self._role = Role.Follower
                    self._voted_for = None
                    break

                if message.voted:
                    self._votes_received.add(message.sender_id)

                # If you have a majority, promote yourself
                if len(self._votes_received) >= (self.node_count + 1) // 2:
                    self._role = Role.Leader
                    break

            if message.type == MessageType.RequestVote:
                if message.term > self._term:
                    # Higher term vote is going on => become follower
                    self._term = message.term
                    self._role = Role.Follower
                    self._voted_for = None

                self._check_and_vote(message)

            elif message.type == MessageType.AppendEntries:
                if message.term >= self._term:
                    # Leader was already chosen on this term => become follower
                    self._term = message.term
                    self._role = Role.Follower
                    self._voted_for = None

                self._check_and_append_entries(message)

            elif message.type == MessageType.ClientRequest:
                self._ask_client_to_wait()

            # Check whether we're still a candidate
            if self._role != Role.Candidate:
                break

            # Election is timed out => start again
            if time.time() - election_started > self._election_timeout:
                return

        return

    def _leader_flow(self):
        print(f'Now leader is : {self.id}')
        self.leader_id = self.id

        self._to_send = [len(self._log) for _ in range(self.node_count)]
        self._acked = [0 for _ in range(self.node_count)]

        last_heartbeat = None

        while not self._stopped.is_set and self._role == Role.Leader:
            # Send heartbeat (at the same time update other nodes' logs)
            if last_heartbeat is None or time.time() - last_heartbeat > self._heartbeat_delta:
                for node_id in range(self.node_count):
                    if node_id == self.id:
                        continue
                    self._send_append_entries_request(node_id)
                last_heartbeat = time.time()

            # Check what we can commit
            while self._committed < len(self._log):
                id_to_commit = self._committed
                acked = 1
                for node_id in range(self.node_count):
                    if node_id != self.id and self._acked[node_id] >= id_to_commit:
                        acked += 1
                if acked >= (self.node_count + 1) // 2:
                    print(f"Leader committed entry {id_to_commit}!")
                    self._commit_to_db(id_to_commit)
                    self._committed += 1
                else:
                    break

            # Check incoming messages
            message = self._get_message()
            if message.type == MessageType.Acknowledgement:
                message = cast(Acknowledgement, message)
                if message.term > self._term:
                    # We're too old => become follower
                    self._term = message.term
                    self._role = Role.Follower
                    break

                if message.ack:
                    self._to_send[message.sender_id] = message.ack
                    self._acked[message.sender_id] = message.ack
                else:
                    # we've sent too much => next time send less
                    self._to_send[message.sender_id] -= 1
                    self._send_append_entries_request(message.sender_id)

            if message.type == MessageType.RequestVote:
                if message.term > self._term:
                    # Higher term vote is going on => become follower
                    self._term = message.term
                    self._role = Role.Follower
                    self._voted_for = None

                self._check_and_vote(message)

            elif message.type == MessageType.ClientRequest:
                self._serve_client(message)

        return

    # ----- Generic message handlers -----

    def _check_and_vote(self, message: RequestVote) -> None:
        '''
        Vote for candidate if he's up to date
        Turn to follower if we voted.
        '''
        my_log_term = (self._log[-1].term if len(self._log) > 0 else 0)
        log_is_ok = (my_log_term < message.log_term or
                     (my_log_term == message.log_term and len(self._log) <= message.log_length))
        term_is_ok = (message.term > self._term or
                      (message.term == self._term and self.voted_for in [None, message.sender_id]))

        if log_is_ok and term_is_ok:
            self._term = message.term
            self._role = Role.Follower
            self._voted_for = message.sender_id
            self._send_vote(True)
        else:
            self._send_vote(False)

    def _check_and_append_entries(self, message: AppendEntries) -> None:
        '''
        Check whether append entries request is newer.
        If so, update our log.
        '''
        # My log should be longer and we should have a common prefix
        log_is_ok = (len(self._log) >= message.log_length and
                     (message.log_length == 0 or message.term == self._log[message.log_length - 1]))
        if message.term == self._term and log_is_ok:
            self._append_entries(message.log_length, message.committed, message.entries)
            self._send_ack(len(message.log_length + len(message.entries)), True)
            return True

        self._send_ack(0, False)
        return False

    # ----- Respond with message -----

    def _send_vote(self, vote_granted: bool) -> None:
        # TODO: implement
        pass

    def _send_ack(self, acked_len: int, acked: bool) -> None:
        # TODO: implement
        pass

    def _broadcast_vote_request(self) -> None:
        my_log_term = (self._log[-1].term if len(self._log) > 0 else 0)
        # TODO: implement
        pass

    def _send_append_entries_request(self, node_id: int):
        log_len = self._to_send[node_id]
        entries = []
        for i in range(log_len, len(self._log)):
            entries.append(self._log(i))
        
        # TODO: implement
        pass

    # ----- Respond to client -----

    def _serve_client(self, message: ClientRequest) -> None:
        operation = message.operation.lower()
        key = message.key
        if operation == 'get':
            if key in self._data:
                self._send_message({key: self._data[key]})
            else:
                self._send_message({key: 'None'})
        elif operation == 'store':
            entry = LogEntry()
            entry.key = key
            entry.value = message.value
            entry.term = self._term
            self._log.append(entry)
            self._send_message('Ok')

        if operation != 'get':
            # We've added an entry to log => update self._to_send
            for node_id in range(self.node_count):
                if node_id == self.id:
                    continue
                if self._to_send[node_id] == len(self._log) - 1:
                    self._to_send[node_id] = len(self._log)

    def _send_client_to_leader(self) -> None:
        '''
        I'm no a leader => tell client leader's address
        '''
        self._send_message(f'Ask leader: {self._leader_id}')

    def _ask_client_to_wait(self) -> None:
        '''
        Election is in progress => try again later
        '''
        self._send_message(f'Election in progress. Ask again later')

    # ----- Helpers -----

    def _get_message() -> BaseMessage:
        # TODO: implement
        pass

    def _send_message(msg) -> None:
        # TODO: implement
        pass

    def _append_entries(self, leader_log_len: int, leader_committed: int, entries: list[LogEntry]):
        # 1. Cut log if we're longer
        if len(self._log) > leader_log_len:
            self._log = self._log[:leader_log_len]
        # 2. Append entries
        for entry in entries:
            self._log.append(entry)
        # 3. Commit if leader already committed
        if leader_committed > self._committed:
            for i in range(self._committed, leader_committed):
                self._commit_to_db(i)
            self._committed = leader_committed


    def _commit_to_db(self, entry_id: int):
        '''
        Apply entry to database
        '''
        key = self._log[entry_id].key
        value = self._log[entry_id].value
        if value is None:
            del self._data[key]
        else:
            self._data[key] = value