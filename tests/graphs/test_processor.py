from crossfilter_dataframe.graphs.dag import DAGProcessor
from crossfilter_dataframe.graphs.dag.utils import get_nodes_traversals
from crossfilter_dataframe.graphs.loader import YAMLLoader

from .examples_fixtures import *


def test_traversals(example1):
    graph = YAMLLoader().load_network(example1['paths']['yaml'])
    dag = DAGProcessor(graph)
    
    nodes_to_be_process_and_test = example1['traversals'].keys()
    
    # for each node, get its out_edges after processing to DAG 
    for at_node in nodes_to_be_process_and_test:
        dag.process(at_node)
        traversals = get_nodes_traversals(dag)
        expected = example1['traversals'][at_node] 
        
        assert traversals == expected, ()
    
    
    
    
    
    
    
    
    
