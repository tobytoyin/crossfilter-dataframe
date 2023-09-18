import os
from collections import namedtuple

from pytest import fixture

TEST_JSON_DIR = 'tests/examples/'
TestOutcome = namedtuple('TestOutcome', 'paths expected_edges')


def get_path(fname: str) -> str:
    return os.path.join(TEST_JSON_DIR, fname)


@fixture
def example1():
    paths = {
        'json': get_path('example1.json'),
        'yaml': get_path('example1.yaml'),
    }

    expected_edges = set(
        [
            ('Table1', 'Table2'),
            ('Table1', 'Table3'),
            ('Table3', 'Table4'),
        ]
    )

    # expected set of traversals nodes
    # when DAG is created AT certain NODE
    expected_traversals = {
        'Table1': set([('ROOT', 'Table1'), ('Table1', 'Table2'), ('Table1', 'Table3'), ('Table3', 'Table4')]), 
        'Table3': set([('ROOT', 'Table3'), ('Table3', 'Table4'), ('Table3', 'Table1'), ('Table1', 'Table2')]), 
        }

    return {'paths': paths, 'edges': expected_edges, 'traversals': expected_traversals}
