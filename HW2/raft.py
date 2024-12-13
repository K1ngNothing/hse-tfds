import time
import random
import threading
import enum
import zmq
from queue import Queue, Empty
from typing import cast

from protocol import ClientRequest, MessageType, BaseMessage, RequestVote, RequestVoteResponse, \
    Acknowledgement, AppendEntries, LogEntry, build_message


class Role(enum.Enum):
    Leader = 0,
    Follower = 1,
    Candidate = 2,
    Test = 3,  # used in tests


def get_address(id: int) -> str:
    local_ip = '127.0.0.1'
    starting_port = 8000
    return local_ip + ':' + str(starting_port + id)


class RaftNode(threading.Thread):
    def __init__(self,
                 id: int,
                 node_count: int,
                 role: Role = Role.Follower,
                 bad_network_connections=set()):
        super().__init__()
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
        self._common_prefix = [0 for _ in range(self.node_count)]
        self._acked_length = [0 for _ in range(self.node_count)]

        # Outside manipulation (used in tests)
        self._stopped = threading.Event()
        self._paused = threading.Event()
        # Which connections should always fail
        self._bad_network_connections: set = bad_network_connections

        # Timing variables
        self._election_timeout = random.uniform(0.1, 0.2)
        self._heartbeat_delta = 0.01
        self._send_timeout = 0.005
        self._receive_timeout = 0.005

        # Messaging
        self._message_queue = Queue()  # Queue for incoming messages
        self._zmq_context = zmq.Context()
        self._receiver_socket = self._zmq_context.socket(zmq.PULL)
        self._receiver_socket.bind(f"tcp://{get_address(self.id)}")
        self._receiver_thread = threading.Thread(
            target=self._receive_messages, daemon=True)
        self._receiver_thread.start()

    def stop(self):
        self._stopped.set()
        self._paused.set()
        # print(f'Node {self.id} is stopped')

    def pause(self):
        self._paused.set()
        print(f'Node {self.id} is paused')

    def unpause(self):
        self._paused.clear()
        # Clear message queue. This will drop all messages received while paused.
        self._message_queue.queue.clear()
        print(f'Node {self.id} is unpaused')

    def run(self):
        while not self._stopped.is_set():
            if self._paused.is_set():
                time.sleep(self._heartbeat_delta)
                continue

            if self._role == Role.Leader:
                self._leader_flow()
            elif self._role == Role.Follower:
                self._follower_flow()
            elif self._role == Role.Candidate:
                self._candidate_flow()
            elif self._role == Role.Test:
                # Just serve client requests
                message = self._get_message()
                if message is not None and message.type == MessageType.ClientRequest:
                    self._serve_client(message)

    # ----- Flows for each role -----

    def _follower_flow(self) -> None:
        if self._role != Role.Follower:
            return

        print(f"Node {self.id} is follower on term {self._term}")

        last_heartbeat = time.time()

        while not self._paused.is_set() and self._role == Role.Follower:
            message = self._get_message()

            if message is not None:
                if message.type == MessageType.RequestVote:
                    if message.term > self._term:
                        # Vote in progress => we should not promote to Candidate
                        last_heartbeat = time.time()

                    # Try to vote for candidate
                    self._check_and_vote(message)

                elif message.type == MessageType.AppendEntries:
                    if message.term > self._term:
                        # We're old => update term
                        self._term = message.term
                        self._voted_for = None

                    if message.term == self._term:
                        self._leader_id = message.sender_id
                        # We've got a message from leader => update timer
                        last_heartbeat = time.time()

                    # Try to append entries
                    self._check_and_append_entries(message)

                elif message.type == MessageType.ClientRequest:
                    message = cast(ClientRequest, message)
                    self._send_client_to_leader(message.address)

            # If you haven't heard a heartbeat in a while, promote yourself to a candidate
            if time.time() - last_heartbeat > self._election_timeout:
                self._role = Role.Candidate
                return

    def _candidate_flow(self):
        if self._role != Role.Candidate:
            return

        self._term += 1
        self._voted_for = self.id
        self._votes_received = set()
        self._votes_received.add(self.id)
        self._broadcast_vote_request()

        print(f'Node {self.id} is candidate on term {self._term}')

        election_started = time.time()

        while not self._paused.is_set() and self._role == Role.Candidate:
            message = self._get_message()

            if message is not None:
                if message.type == MessageType.RequestVoteResponse:
                    message = cast(RequestVoteResponse, message)

                    if message.term > self._term:
                        # Someone has a higher term => become follower
                        self._demote_to_follower(message.term)
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
                        self._demote_to_follower(message.term)

                    self._check_and_vote(message)

                elif message.type == MessageType.AppendEntries:
                    if message.term >= self._term:
                        # Leader was already chosen on this term => become follower
                        self._demote_to_follower(message.term)

                    self._check_and_append_entries(message)

                elif message.type == MessageType.ClientRequest:
                    message = cast(ClientRequest, message)
                    self._ask_client_to_wait(message.address)

            # Check whether we're still a candidate
            if self._role != Role.Candidate:
                break

            # Election is timed out => start again
            if time.time() - election_started > self._election_timeout:
                return

        return

    def _leader_flow(self):
        if self._role != Role.Leader:
            return

        self._leader_id = self.id
        self._common_prefix = [len(self._log) for _ in range(self.node_count)]
        self._acked_length = [0 for _ in range(self.node_count)]

        print(f'!!! Node {self.id} is leader on term {self._term}')

        last_heartbeat = None

        while not self._paused.is_set() and self._role == Role.Leader:
            # Send heartbeat (at the same time update other nodes' logs)
            if last_heartbeat is None or time.time() - last_heartbeat > self._heartbeat_delta:
                for node_id in range(self.node_count):
                    if node_id == self.id:
                        continue
                    self._send_append_entries_request(node_id)
                last_heartbeat = time.time()

            # Check what we can commit
            self._try_to_commit_entries()

            # Check incoming messages
            message = self._get_message()

            if message is not None:
                if message.type == MessageType.Acknowledgement and message.term >= self._term: # i.e. this ACK is relevant
                    message = cast(Acknowledgement, message)
                    if message.term > self._term:
                        # We're too old => become follower
                        self._demote_to_follower(message.term)
                        break

                    if message.ack:
                        self._common_prefix[message.sender_id] = message.acked
                        self._acked_length[message.sender_id] = message.acked
                    elif message.acked == self._common_prefix[message.sender_id]:  # i.e. it's up-to-date NACK
                        # we've sent too much => next time send less
                        self._common_prefix[message.sender_id] -= 1

                if message.type == MessageType.RequestVote:
                    if message.term > self._term:
                        # Higher term vote is going on => become follower
                        self._demote_to_follower(message.term)

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
                      (message.term == self._term and self._voted_for in [None, message.sender_id]))

        if log_is_ok and term_is_ok:
            self._term = message.term
            self._role = Role.Follower
            self._voted_for = message.sender_id
            self._send_vote(message.sender_id, True)
        else:
            self._send_vote(message.sender_id, False)

    def _check_and_append_entries(self, message: AppendEntries) -> None:
        '''
        Check whether append entries request is newer.
        If so, update our log.
        '''
        if message.term >= self._term:
            self._demote_to_follower(message.term)
            self._leader_id = message.sender_id

        # My log should be longer and we should have a common prefix
        log_is_ok = (len(self._log) >= message.log_length and
                     (message.log_length == 0 or message.log_term == self._log[message.log_length - 1].term))
        if message.term == self._term and log_is_ok:
            self._append_entries(message.log_length,
                                 message.committed, message.entries)
            self._send_ack(message.sender_id, True,
                           message.log_length + len(message.entries))
        else:
            self._send_ack(message.sender_id, False, message.log_length)

    def _append_entries(self, leader_log_len: int, leader_committed: int, entries: list[LogEntry]):
        # 1. Cut log if we're longer
        if len(self._log) > leader_log_len:
            self._log = self._log[:leader_log_len]
        # 2. Append entries
        for entry in entries:
            self._log.append(entry)
        # 3. Commit what leader already committed
        if leader_committed > self._committed:
            for i in range(self._committed, leader_committed):
                self._commit_to_db(i)
            self._committed = leader_committed

    def _try_to_commit_entries(self) -> None:
        if self._role != Role.Leader:
            return

        while self._committed < len(self._log):
            id_to_commit = self._committed
            acked = 1
            for node_id in range(self.node_count):
                if node_id != self.id and self._acked_length[node_id] >= id_to_commit:
                    acked += 1
            if acked >= (self.node_count + 1) // 2:
                self._commit_to_db(id_to_commit)
                self._committed += 1
            else:
                break

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

    def _demote_to_follower(self, term: int):
        if self._term < term:
            print(f"Node {self.id} was demoted to follower on term {term}")

        self._term = term
        self._role = Role.Follower
        self._voted_for = None

    # ----- Respond to client -----

    def _serve_client(self, message: ClientRequest) -> None:
        operation = message.operation.lower()
        key = message.key
        reply_to_client: str | dict = "Empty response"
        init_log_len = len(self._log)

        # Do request and prepare reply
        if operation == 'get':
            if key in self._data:
                reply_to_client = {key: self._data[key]}
            else:
                reply_to_client = {key: 'None'}
        elif operation == 'store':
            entry = LogEntry(self._term, key, message.value)
            self._log.append(entry)
            reply_to_client = 'Ok'
        elif operation == 'delete':
            entry = LogEntry(self._term, key, None)
            self._log.append(entry)
            reply_to_client = 'Ok'
        elif operation == 'inc':
            if key not in self._data:
                reply_to_client = 'Key not found :('
            elif type(self._data[key]) != int or type(message.value) != int:
                reply_to_client = "Stored or passed value isn't int"
            else:
                new_value = self._data[key] + int(message.value)
                entry = LogEntry(self._term, key, new_value)
                self._log.append(entry)
                reply_to_client = 'Ok'
        else:
            reply_to_client = 'Only support operations: get, store, delete, inc'

        # Send reply to client
        self._reply_to_client(message.address, reply_to_client)

        if len(self._log) > init_log_len:
            # We've added an entry to log => send append entries request to all nodes
            # (but commit can only be done at least at the next leader's cycle)
            for node_id in range(self.node_count):
                if node_id == self.id:
                    continue

                self._send_append_entries_request(node_id)

    def _send_client_to_leader(self, client_addr: str) -> None:
        ''' I'm not a leader => tell client leader's address '''
        self._reply_to_client(client_addr, f'Ask leader: {self._leader_id}')

    def _ask_client_to_wait(self, client_addr: str) -> None:
        ''' Election is in progress => ask client to try again later '''
        self._reply_to_client(client_addr, f'Election in progress. Ask again later.')

    # ----- Respond with message -----

    def _send_vote(self, target_id: int, vote_granted: bool) -> None:
        response = RequestVoteResponse({
            'term': self._term,
            'sender_id': self.id,

            'voted': vote_granted
        })
        self._send_message(target_id, response.to_dict())

    def _send_ack(self, target_id: int, ack: bool, acked_len: int) -> None:
        response = Acknowledgement({
            'term': self._term,
            'sender_id': self.id,

            'ack': ack,
            'acked': acked_len
        })
        self._send_message(target_id, response.to_dict())

    def _broadcast_vote_request(self) -> None:
        my_log_term = (self._log[-1].term if len(self._log) > 0 else 0)
        request = RequestVote({
            'term': self._term,
            'sender_id': self.id,

            'log_term': my_log_term,
            'log_length': len(self._log)
        })
        request_dict = request.to_dict()

        for node_id in range(self.node_count):
            if node_id == self.id:
                continue
            self._send_message(node_id, request_dict)

    def _send_append_entries_request(self, node_id: int):
        log_length = self._common_prefix[node_id]
        log_term: int = 0
        if len(self._log) > 0 and log_length > 0:
            log_term = self._log[log_length - 1].term
        entries = []
        for i in range(log_length, len(self._log)):
            entries.append(self._log[i])

        request = AppendEntries({
            'term': self._term,
            'sender_id': self.id,

            'log_term': log_term,
            'log_length': log_length,
            'committed': self._committed,
            'entries': entries
        })
        self._send_message(node_id, request.to_dict())

    # ----- Messaging -----

    def _receive_messages(self):
        '''
        Continuously receive messages and put them into the message queue.
        '''
        while not self._stopped.is_set():
            try:
                msg = self._receiver_socket.recv_json()
                self._message_queue.put(msg)

            except:
                pass

    def _get_message(self) -> BaseMessage:
        '''
        Retrieve the next message from the message queue.
        If no message is found in timeout, return None
        '''
        while not self._stopped.is_set():
            try:
                msg = self._message_queue.get(timeout=self._receive_timeout)
                return build_message(msg)
            except Empty:
                return None

    def _send_message(self, target_id: int, msg: str | dict) -> None:
        '''
        Send a message to a specific node.
        '''
        try:
            target_address = get_address(target_id)
            with self._zmq_context.socket(zmq.PUSH) as socket:
                socket.connect(f"tcp://{target_address}")
                socket.setsockopt(zmq.SNDTIMEO, int(self._send_timeout * 1000))
                if isinstance(msg, dict):
                    socket.send_json(msg)
                else:
                    socket.send_string(msg)
        except:
            pass

    def _reply_to_client(self, address: str, msg: str | dict) -> None:
        try:
            with self._zmq_context.socket(zmq.PUSH) as socket:
                socket.connect(f"tcp://{address}")
                socket.setsockopt(zmq.SNDTIMEO, int(self._send_timeout * 1000))
                if isinstance(msg, dict):
                    socket.send_json(msg)
                else:
                    socket.send_string(msg)
        except:
            pass
