import os
import time
import random

# My files
from test_utils import *
from node import Node

def test_random_delays():
    node_count = 3
    nodes = [Node(id = i, node_count=node_count, random_delay=True)
             for i in range(node_count)]
    time.sleep(0.1)

    # Do some operations with different nodes.
    random.seed(42)
    request_count = 20
    keys = ['a', 'b', 'c']
    for _ in range(request_count):
        node_id = random.choice(range(node_count))
        key = random.choice(keys)
        value = random.choice([1, 2, 3, None])
        request_patch_1(nodes[node_id], key, value)

    # Wait a bit and than check that results for all nodes are the same
    time.sleep(3)
    for key in keys:
        golden = None
        for node in nodes:
            node_result = request_get(node, key)
            assert golden is None or node_result == golden,\
                   f"Nodes aren't consistent for key {key}: golden = {golden}, result from node {node.id} = {node_result}"
            if golden is None:
                golden = node_result
    print("Test passed!")


def test_node_crashing():
    node_count = 3
    nodes = [Node(id = i, node_count=node_count, random_delay=True)
             for i in range(node_count)]
    time.sleep(0.1)

    # Do request to node 0
    request_patch(nodes[0], [
        {KEY: 'a', VALUE: 0},
        {KEY: 'b', VALUE: 0}
    ])
    time.sleep(0.2)
    assert request_get(nodes[1], 'a') == 0
    assert request_get(nodes[1], 'b') == 0

    # Isolate node 0. Do requests to nodes 1 and 2
    nodes[0].sleep(1)
    request_patch(nodes[1], [
        {KEY: 'a', VALUE: 1},
    ])
    request_patch(nodes[2], [
        {KEY: 'b', VALUE: 1},
    ])
    time.sleep(2)
    assert request_get(nodes[1], 'b') == 1
    assert request_get(nodes[2], 'a') == 1

    # Node 1 must've woke up and synchronized - check values 'a' and 'b'
    assert request_get(nodes[0], 'a') == 1
    assert request_get(nodes[0], 'b') == 1

    print("Test passed!")


# DEBUG
def interactive_mode(random_delay: bool = True):
    node_count = 3
    nodes = [Node(id = i, node_count=node_count, random_delay=random_delay)
             for i in range(node_count)]
    time.sleep(0.1)

    print("Input command: (GET, node_id, key), (PATCH, node_id, key, value) or (SLEEP, node_id, timeout)")
    while True:
        try:
            data = input().split()
            operation = data[0].lower()
            node_id = int(data[1])
            node = nodes[node_id]

            if operation == 'get':
                key = data[2]
                print(request_get(node, key))
            elif operation == 'patch':
                key = data[2]
                value = data[3]
                print(request_patch(node, [{KEY: key, VALUE: value}]))
            elif operation == 'sleep':
                timeout = float(data[2])
                node.sleep(timeout)
            else:
                print("Unknown operation")
        except KeyboardInterrupt:
            os._exit(0)
        except Exception as e:
            print(f"Some error occurred: {e}")


if __name__ == "__main__":
    # test_random_delays()
    test_node_crashing()
