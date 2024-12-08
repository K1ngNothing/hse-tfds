import subprocess
import time
import requests
import sys
import os

REPLICA_FILE = "replica.py"


def start_server(port: int):
    process = subprocess.Popen([sys.executable, REPLICA_FILE, "--port", str(port)],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    return process


def test_simple():
    port = 5000
    url = f"http://127.0.0.1:{port}"
    server = start_server(port)
    time.sleep(2)

    # 1. Create
    response = requests.post(url, json=42)
    assert response.status_code == 201
    assert response.json()['data'] == 42
    id = response.json()['id']

    # 2. Read
    response = requests.get(f"{url}/{id}")
    assert response.status_code == 200
    assert response.json()['data'] == 42

    # 3. Update
    response = requests.put(f"{url}/{id}", json=43)
    assert response.status_code == 200
    assert response.json()['data'] == 43

    # 4. Delete
    response = requests.delete(f"{url}/{id}")
    assert response.status_code == 204
    response = requests.get(f"{url}/{id}")
    assert response.status_code == 404

    server.terminate()

if __name__ == "__main__":
    test_simple()
