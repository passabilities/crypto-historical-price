from threading import Timer
import collections
import json

__queue = []

def queue(file_name, data):
    __queue.append([file_name, data])

def _merge(a, b):
    for k, v in b.items():
        if (k in a and isinstance(a[k], dict) and isinstance(b[k], collections.Mapping)):
            _merge(a[k], b[k])
        else:
            a[k] = b[k]

    return a

def __write():
    try:
        file_name, data = __queue.pop()
        try:
            with open(file_name, 'r') as file:
                current_data = json.load(file)
        except FileNotFoundError:
            current_data = {}

        with open(file_name, 'w') as file:
            json.dump(_merge(current_data, data), file)

        __write()
    except IndexError:
        t = Timer(0.1, __write)
        t.start()

__write()
