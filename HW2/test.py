import time
import zmq
import json
import os
from raft import RaftNode, Role, get_address
from protocol import ClientRequest, LogEntry


def find_leader(nodes: list[RaftNode], exclude: list = []) -> int | None:
    leader = None
    for (i, node) in enumerate(nodes):
        if i not in exclude and node._role == Role.Leader:
            assert leader is None, "We've got 2 leaders :("
            leader = i
    return leader


def request_node(node_id: int, request_dict: dict) -> dict | str:
    my_address = '127.0.0.1:9000'
    request_dict['address'] = my_address

    zmq_context = zmq.Context()
    sender_socket = zmq_context.socket(zmq.PUSH)
    receiver_socket = zmq_context.socket(zmq.PULL)

    receiver_socket.bind(f"tcp://{my_address}")
    receive_timeout = 1  # sec
    receiver_socket.setsockopt(zmq.RCVTIMEO, receive_timeout * 1000)

    try:
        target_address = get_address(node_id)
        sender_socket.connect(f"tcp://{target_address}")
        sender_socket.send_json(request_dict)

        reply_bytes = receiver_socket.recv()

        # Reply can be either dict or str
        try:
            reply = json.loads(reply_bytes.decode('utf-8'))
        except json.JSONDecodeError:
            reply = reply_bytes.decode('utf-8')

        return reply
    except zmq.error.Again:
        return None
    finally:
        sender_socket.close()
        receiver_socket.close()
        zmq_context.term()


def test_leader_election():
    node_count = 5
    nodes = [RaftNode(id=i, node_count=node_count) for i in range(node_count)]
    for node in nodes:
        node.start()
    time.sleep(1)

    try:
        # 1. Pause all nodes and find leader
        for node in nodes:
            node.pause()
        first_leader = find_leader(nodes)
        assert first_leader is not None, "First leader wasn't elected :("
        print(f"First leader: {first_leader}")

        # 2. Unpause everyone except leader. Wait some time for a new leader to be elected.
        for (i, node) in enumerate(nodes):
            if i != first_leader:
                node.unpause()
        time.sleep(1)

        second_leader = find_leader(nodes, [first_leader])
        assert second_leader is not None, "Second leader wasn't elected :("
        print(f"Second leader: {second_leader}")

        # 3. Unpause first leader. He now should become a follower
        nodes[first_leader].unpause()
        time.sleep(1)
        assert nodes[first_leader]._role == Role.Follower, \
            f"First leader's role is {nodes[first_leader]._role}"
        print('Success!')

    finally:
        # Stop every node
        for node in nodes:
            node.stop()


def test_multiple_candidates():
    node_count = 5
    nodes = [RaftNode(id=i, node_count=node_count) for i in range(node_count)]
    nodes[0]._role = Role.Candidate
    nodes[1]._role = Role.Candidate
    nodes[2]._role = Role.Candidate
    for node in nodes:
        node.start()
    time.sleep(1)

    try:
        for node in nodes:
            node.pause()
        first_leader = find_leader(nodes)
        assert first_leader is not None, "First leader wasn't elected :("

    finally:
        # Stop every node
        for node in nodes:
            node.stop()


def test_replication():
    node_count = 5
    nodes = [RaftNode(id=i, node_count=node_count) for i in range(node_count)]
    nodes[0]._role = Role.Leader  # For simplicity sake we set first leader
    for node in nodes:
        node.start()
    time.sleep(1)

    try:
        # 1. Store some value
        result = request_node(0, {
            'operation': 'store',
            'key': 'kek',
            'value': 42
        })
        assert result == 'Ok'
        time.sleep(0.5)

        # 2. Stop current leader and elect a new one
        nodes[0].pause()
        time.sleep(1)
        new_leader = find_leader(nodes, [0])
        assert new_leader is not None, "No leader is elected :("

        # 3. Check stored value and change it
        result = request_node(new_leader, {
            'operation': 'get',
            'key': 'kek'
        })
        assert result == {'kek': 42}, "Store operation wasn't replicated"

        result = request_node(new_leader, {
            'operation': 'inc',
            'key': 'kek',
            'value': 1
        })
        assert result == 'Ok'
        time.sleep(0.5)

        # 4. Inject wrong log entry to the first leader's log as if 'kek' was deleted
        nodes[0]._log.append(LogEntry(
            term=nodes[0]._term,
            key='kek',
            value=None)
        ) 

        # 5. Unpause first leader and wait for synchronization
        nodes[0].unpause()
        time.sleep(1)

        # 6. Check what value is stored in first leader
        nodes[0].pause()
        time.sleep(0.1)
        assert 'kek' in node._data and node._data['kek'] == 43, \
               "Node 0 didn't synchronize with others"

        print("Success!")
        os._exit(0)  # For some reason test may hang

    finally:
        # Stop every node
        for node in nodes:
            node.stop()


def interactive_mode():
    node_count = 5
    nodes = [RaftNode(id=i, node_count=node_count) for i in range(node_count)]
    for node in nodes:
        node.start()
    time.sleep(1)

    print("Input command: (operation, node_id, key, value)")
    print("Supported operations: get, store, delete, inc")
    while True:
        try:
            data = input().split()
            operation = data[0].lower()
            node_id = int(data[1])

            if operation == 'kill':
                nodes[node_id].pause()
                print(f"! Killed node {node_id}")
            elif operation == 'revive':
                nodes[node_id].unpause()
                print(f"! Revived node {node_id}")
            elif operation in ['get', 'store', 'delete', 'inc']:
                key = data[2]
                value = (data[3] if len(data) >= 4 else None)
                if value is not None:
                    value = int(value)
                print(f"! Sending request to node {node_id}: {operation}, key={key}, value={value}")
                result = request_node(node_id, {
                    'operation': operation,
                    'key': key,
                    'value': value
                })
                print(result)
            else:
                print("Unknown operation")
        except KeyboardInterrupt:
            os._exit(0)
        except:
            print("Some error occurred")


if __name__ == "__main__":
    # test_leader_election()
    # test_multiple_candidates()
    # test_replication()
    interactive_mode()
