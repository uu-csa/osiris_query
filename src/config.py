# standard library
import json
from pathlib import Path
from collections import namedtuple


PATH_LIB = Path(__file__).resolve().parent.parent
PATH_CONFIG = PATH_LIB / 'config'
PATH_OUTPUT = PATH_LIB / 'output'
PATH_INPUT = PATH_LIB / 'queries'
PATH_LOGIN = Path('u:/uustprd.txt')


def load_registry(filename):
    # load parameters
    with open(filename, 'r', encoding='utf8') as f:
        return json.load(f)


def register(filename, section, queries, parameters):
    registry = dict()
    if filename.exists():
        registry = load_registry(filename)
    registry[section] = {
        'parameters': parameters,
        'queries': queries,
        }
    with open (filename, 'w', encoding='utf8') as f:
        f.write(json.dumps(registry, indent=4))
    return None
