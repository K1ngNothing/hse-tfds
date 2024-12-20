import requests
from typing import Any

# My files
from messages import *
from node import Node

SUCCESS = 'success'

def request_get(node: Node, key: str) -> Any | str:
    '''
    Do GET request to node.

    Returns:
        Any: VALUE field if request was successful
        str: error description if it wasn't
    '''
    url = f'{node.get_http_address()}/{key}'
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data[VALUE]
        else:
            return f"Error {response.status_code}: {response.text}"

    except requests.exceptions.RequestException as e:
        return f"RequestException: {e}"


def request_patch(node: Node, operations: list) -> str:
    '''
    Do PATCH request to node.

    Returns:
        str: SUCCESS if request was successful, error description if it wasn't
    '''

    url = f'{node.get_http_address()}/'
    payload = {OPERATIONS: operations}
    try:
        response = requests.patch(url, json=payload)
        if response.status_code == 200:
            return SUCCESS
        else:
            return f"Error {response.status_code}: {response.text}"

    except requests.exceptions.RequestException as e:
        return f"RequestException: {e}"

def request_patch_1(node: Node, key: str, value: Any, verbose: bool = False) -> None:
    '''
    Do PATCH request with 1 operation to node.

    Returns:
        str: SUCCESS if request was successful, error description if it wasn't
    '''
    if verbose:
        print(f"Request to node {node.id}: key = {key}, value = {value}")
    assert request_patch(node, [{KEY: key, VALUE: value}]), \
           'Patch request has failed'
