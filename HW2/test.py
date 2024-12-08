import time
import zmq
import json
from raft import RaftNode, Role, get_address
from protocol import ClientRequest


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
    receive_timeout = 5  # sec
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
        return "Timeout: no response received"
    finally:
        sender_socket.close()
        receiver_socket.close()
        zmq_context.term()


def test_leader_election():
    node_count = 5
    nodes = [RaftNode(id=i, node_count=node_count) for i in range(node_count)]
    for node in nodes:
        node.start()
    time.sleep(2)

    # Pause all and find leader
    for node in nodes:
        node.pause()
    first_leader = find_leader(nodes)
    assert first_leader is not None, "No leader is elected :("

    # Unpause everyone except leader. Wait sometime for new leader to be elected.
    for (i, node) in enumerate(nodes):
        if i != first_leader:
            node.unpause()
    time.sleep(2)

    # Unpause old leader. He now should become a follower
    nodes[first_leader].unpause()
    time.sleep(2)
    assert nodes[first_leader]._role == Role.Follower

    # Stop every node
    for node in nodes:
        node.stop()


def test_operations():
    node_count = 5
    nodes = [RaftNode(id=i, node_count=node_count) for i in range(node_count)]
    nodes[0]._role = Role.Leader  # For simplicity sake we set first leader
    for node in nodes:
        node.start()
    time.sleep(2)

    result = request_node(0, {
        'operation': 'store',
        'key': 'kek',
        'value': 42
    })
    assert result == 'Ok'
    time.sleep(0.1)  # Actually not necessary, but it's possible that previous entry is not yet committed

    result = request_node(0, {
        'operation': 'get',
        'key': 'kek'
    })
    assert result == {'kek': 42}

    result = request_node(0, {
        'operation': 'delete',
        'key': 'kek',
        'value': None
    })
    assert result == 'Ok'
    time.sleep(0.1)

    # Stop current leader and elect new one
    nodes[0].pause()
    time.sleep(1)
    new_leader = find_leader(nodes, [0])
    assert new_leader is not None, "No leader is elected :("

    result = request_node(new_leader, {
        'operation': 'get',
        'key': 'kek'
    })
    assert result == {'kek': 'None'}  # key was indeed deleted

    # Test that outdated replic will synchronize with new leader
    # We will unpause node[0], wait a bit, and than pause the whole system
    # After that we will manually serve request through node[0]
    nodes[0].unpause()
    time.sleep(1)
    for node in nodes:
        node.pause()
    nodes[0]._role = Role.Leader  # otherwise replic will not serve the request
    nodes[0].unpause()

    result = request_node(new_leader, {
        'operation': 'get',
        'key': 'kek'
    })
    assert result == {'kek': 'None'}  # key was indeed deleted

    # Stop every node
    for node in nodes:
        node.stop()


if __name__ == "__main__":
    # test_leader_election()
    test_operations()
