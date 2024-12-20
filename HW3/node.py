from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, cast
import json
import random
import threading
import time
import zmq

# My files
import vector_clock
from messages import *

# Alias used for type hints
Message = dict[str, Any]

def get_node_address(node_id: int) -> str:
    local_ip = '127.0.0.1'
    starting_port = 5050
    return local_ip + ':' + str(starting_port + node_id)

class Node:
    def __init__(self,
                 id: int,
                 node_count: int,
                 random_delay: bool = False):
        self.id: int = id
        self.node_count: int = node_count
        self.data: dict[str, dict[str, Any]] = {} # key -> value, timestamp, sender_id

        # Clock
        self.clock: list[int] = [0 for _ in range(self.node_count)]

        # Messages
        self.msg_processing_lock = threading.Lock()
        self.received_messages: set = set()
        self.last_msg_id: int = -1

        # Communication with nodes
        self.zmq_context = zmq.Context()
        self.zmq_socket = self.zmq_context.socket(zmq.PULL)
        self.zmq_socket.bind(f"tcp://{get_node_address(self.id)}")
        self.zmq_thread = threading.Thread(
            target=self.handle_messages_from_nodes, daemon=True)
        self.zmq_thread.start()

        # Communication with user
        self.http_port = 8000 + id
        self.http_thread = threading.Thread(
            target=self.run_http_server, daemon=True)
        self.http_thread.start()

        # For tests
        self.random_message_delay: bool = random_delay

    # ----- Main logic -----

    def process_client_request(self, message: dict) -> None:
        with self.msg_processing_lock:
            # 1. Update clock
            self.clock[self.id] += 1

            # 2. Create message to be broadcasted
            message[MESSAGE_TYPE] = CLIENT_REQUEST
            message[SENDER_ID] = self.id
            message[TIMESTAMP] = self.clock
            message[MESSAGE_ID] = self.last_msg_id + 1
            self.last_msg_id += 1

        # 3. Do client request
        self.process_message(message)

        # 4. Send message to myself for asynchronous broadcasting 
        self.send_message(message, self.id)

    def handle_messages_from_nodes(self) -> None:
        '''
        Receive and handle messages from node (including ourselves)
        '''
        while True:
            message = self.zmq_socket.recv_json()

            if message[MESSAGE_TYPE] == CLIENT_REQUEST:
                if self.check_and_mark_as_received(message):
                    # Message was already received
                    continue

                if message[SENDER_ID] != self.id: # Otherwise message was already processed
                    self.process_message(message)
                self.broadcast(message)

            # Special messages used in tests
            elif message[MESSAGE_TYPE] == SLEEP:
                print(f'Node {self.id} is sleeping')
                time.sleep(message[TIMEOUT])
                print(f'Node {self.id} woke up')

    def process_message(self, message: dict) -> None:
        with self.msg_processing_lock:
            sender_id: int = message[SENDER_ID]
            if sender_id != self.id:
                # Update our clock
                self.clock = vector_clock.merge(self.clock, message[TIMESTAMP])
                self.clock[self.id] += 1

            # Process operations
            for operation in message[OPERATIONS]:
                operation = cast(dict, operation)
                key = operation[KEY]
                new_value = operation[VALUE]
                timestamp = message[TIMESTAMP]

                if not self.should_apply_operation(message, key):
                    assert sender_id != self.id, "Didn't apply operation from myself"
                    continue

                self.data[key] = {
                    VALUE: new_value,
                    TIMESTAMP: timestamp,
                    SENDER_ID: sender_id
                }

    def check_and_mark_as_received(self, message: Message) -> bool:
        '''
        Checks whether message was already received. If not, mark as received.

        Returns:
            bool: whether message was already received BEFORE this function call
        '''
        with self.msg_processing_lock:
            msg_unique_id = get_message_unique_id(message)
            if msg_unique_id in self.received_messages:
                return True

            self.received_messages.add(msg_unique_id)
            return False

    def should_apply_operation(self, request: dict, key: str) -> bool:
        if key not in self.data:
            return True

        cur_value = self.data[key]
        if vector_clock.isLEQ(cur_value[TIMESTAMP], request[TIMESTAMP]):
            return True

        if vector_clock.isLEQ(request[TIMESTAMP], cur_value[TIMESTAMP]):
            return False

        # Timestamps aren't comparable => resolve with sender_id
        return request[SENDER_ID] < cur_value[SENDER_ID]

    # ----- Helpers -----

    def send_message(self, message: Message, node_id: int) -> None:
        '''
        Reliably sends message to other node. Supports random delay for testing.
        '''
        if self.random_message_delay and node_id != self.id:
            time.sleep(random.uniform(0.01, 0.05))

        try:
            sender_socket = self.zmq_context.socket(zmq.PUSH)
            sender_socket.connect(f"tcp://{get_node_address(node_id)}")
            sender_socket.send_json(message)
        except Exception:
            pass
        finally:
            if sender_socket is not None:
                sender_socket.close()

    def broadcast(self, message: Message) -> None:
        '''
        Best effort broadcast
        '''
        for node_id in range(self.node_count):
            if node_id != self.id:
                self.send_message(message, node_id)

    # ----- Test handles -----

    def sleep(self, timeout: float) -> None:
        '''
        Make node sleep for some time. Used in tests.
        '''
        self.send_message({MESSAGE_TYPE: SLEEP, TIMEOUT: timeout}, self.id)

    # ----- Runs HTTP server -----

    def run_http_server(self) -> None:
        self.http_server = HTTPServer(('localhost', self.http_port), lambda *args: HTTPRequestHandler(self, *args)) # (?)
        print(f'Node {self.id}: HTTP server is running')
        self.http_server.serve_forever()

    def get_http_address(self) -> str:
        return f'http://localhost:{self.http_port}'


class HTTPRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, node: Node, *args):
        self.node = node
        super().__init__(*args)

    def do_GET(self) -> None:
        key = self.path[1:]  # Cut '/' symbol
        with self.node.msg_processing_lock:
            if key in self.node.data:
                code = 200
                value = self.node.data[key][VALUE]
                message = {KEY: key, VALUE: value}
            else:
                code = 404
                message = {MESSAGE: 'Key not found'}

        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(message).encode())

    def do_PATCH(self) -> None:
        content_length = int(self.headers['Content-Length'])
        request_data = self.rfile.read(content_length)
        try:
            request_json = json.loads(request_data)
            if OPERATIONS not in request_json:
                raise ValueError("No operations passed")
            if not isinstance(request_json[OPERATIONS], list):
                raise ValueError("Operations must be a list")

            self.node.process_client_request(request_json)
            code = 200
            message = {MESSAGE: 'OK'}

        except (json.JSONDecodeError, ValueError) as e:
            code = 400
            message = {MESSAGE: str(e)}

        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(message).encode())


    def log_message(self, format: str, *args) -> None:
        # Disable logging
        pass
