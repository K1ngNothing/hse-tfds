import time
from raft import RaftNode, Role
from protocol import ClientRequest

def find_leader(nodes: list[RaftNode]) -> int | None:
    leader = None
    for (i, node) in enumerate(nodes):
        if node._role == Role.Leader:
            assert leader is None, "We've got 2 leaders :("
            leader = i
    return leader

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

if __name__ == "__main__":
    test_leader_election()
