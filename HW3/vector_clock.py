# Clock is list of ints. Intended to be imported as a module for naming clarity. 

def merge(a: list[int], b: list[int]):
    assert(len(a) == len(b))
    return [max(a[i], b[i]) for i in range(len(a))]

def isLEQ(a: list[int], b: list[int]):
    assert(len(a) == len(b))
    return all(a[i] <= b[i] for i in range(len(a)))
