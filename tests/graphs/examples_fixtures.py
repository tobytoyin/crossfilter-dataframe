import os
from collections import namedtuple

from pytest import fixture

TEST_JSON_DIR = 'tests/examples/specs/'
TestOutcome = namedtuple('TestOutcome', 'paths expected_edges')


def get_path(fname: str) -> str:
    return os.path.join(TEST_JSON_DIR, fname)


@fixture
def example1():
    # same specs, different file types
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


@fixture
def one_to_many():
    path = get_path('one-to-many.yaml')
    
    expected_edges = set([
        ('Table1', 'Table2'), 
        ('Table1', 'Table3'),
        ('Table1', 'Table4'),
    ])
    
    expected_traversals = {
        'Table1': set([('ROOT', 'Table1'), ('Table1', 'Table2'), ('Table1', 'Table3'), ('Table1', 'Table4')]),
        'Table2': set([('ROOT', 'Table2'), ('Table2', 'Table1'), ('Table1', 'Table3'), ('Table1', 'Table4')]),
        'Table3': set([('ROOT', 'Table3'), ('Table3', 'Table1'), ('Table1', 'Table2'), ('Table1', 'Table4')]),
        'Table4': set([('ROOT', 'Table4'), ('Table4', 'Table1'), ('Table1', 'Table2'), ('Table1', 'Table3')]),
    }
    return {'paths': path, 'edges': expected_edges, 'traversals': expected_traversals}


@fixture
def many_to_one():
    path = get_path('many-to-one.yaml')
    
    expected_edges = set([
        ('Table1', 'Table4'), 
        ('Table2', 'Table4'),
        ('Table3', 'Table4'),
    ])
    
    expected_traversals = {
        'Table1': set([('ROOT', 'Table1'), ('Table1', 'Table4'), ('Table4', 'Table2'), ('Table4', 'Table3')]),
        'Table2': set([('ROOT', 'Table2'), ('Table2', 'Table4'), ('Table4', 'Table1'), ('Table4', 'Table3')]),
        'Table3': set([('ROOT', 'Table3'), ('Table3', 'Table4'), ('Table4', 'Table1'), ('Table4', 'Table2')]),
        'Table4': set([('ROOT', 'Table4'), ('Table4', 'Table1'), ('Table4', 'Table2'), ('Table4', 'Table3')]),
    }
    return {'paths': path, 'edges': expected_edges, 'traversals': expected_traversals}

    
@fixture
def graph_3_1_1():
    path = get_path('3-1-1.yaml')
    
    expected_edges = set([
        ('Table2', 'Table5'), 
        ('Table3', 'Table5'),
        ('Table5', 'Table6'),
        ('Table4', 'Table6'),
    ])
    
    expected_traversals = {
        'Table2': set([('ROOT', 'Table2'), 
                       ('Table2', 'Table5'), 
                       ('Table5', 'Table3'), ('Table5', 'Table6'),
                       ('Table6', 'Table4')]),
    }
    return {'paths': path, 'edges': expected_edges, 'traversals': expected_traversals}    