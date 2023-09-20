from crossfilter_dataframe.graphs.dag import DAGProcessor
from crossfilter_dataframe.graphs.dag.utils import get_nodes_traversals
from crossfilter_dataframe.graphs.loaders import YAMLLoader

from .examples_fixtures import *


def test_traversals(example1):
    graph = YAMLLoader().load_network(example1['paths']['yaml'])
    dag = DAGProcessor(graph)
    
    # This contains a list of Nodes to start a DAG with
    nodes_to_be_process_and_test = iter(example1['traversals'])
    
    # for each node, get its out_edges after processing to DAG 
    for at_node in nodes_to_be_process_and_test:
        dag.process(at_node)
        traversals = get_nodes_traversals(dag)
        expected = example1['traversals'][at_node] 
        
        assert traversals == expected, "Expected Set of Traversals is different"
        
        
def test_one_to_many_traversals(one_to_many):
    fixture = one_to_many
    
    graph = YAMLLoader().load_network(fixture['paths'])
    dag = DAGProcessor(graph)
    
    # This contains a list of Nodes to start a DAG with
    nodes_to_be_process_and_test = iter(fixture['traversals'])
    
    # for each node, get its out_edges after processing to DAG 
    for at_node in nodes_to_be_process_and_test:
        dag.process(at_node)
        traversals = get_nodes_traversals(dag)
        expected = fixture['traversals'][at_node] 
        
        assert traversals == expected, "Expected Set of Traversals is different"

        
def test_many_to_one_traversals(many_to_one):
    fixture = many_to_one
    
    graph = YAMLLoader().load_network(fixture['paths'])
    dag = DAGProcessor(graph)
    
    # This contains a list of Nodes to start a DAG with
    nodes_to_be_process_and_test = iter(fixture['traversals'])
    
    # for each node, get its out_edges after processing to DAG 
    for at_node in nodes_to_be_process_and_test:
        dag.process(at_node)
        traversals = get_nodes_traversals(dag)
        expected = fixture['traversals'][at_node] 
        
        assert traversals == expected, "Expected Set of Traversals is different"        

        
def test_3_1_1(graph_3_1_1):
    fixture = graph_3_1_1
    
    graph = YAMLLoader().load_network(fixture['paths'])
    dag = DAGProcessor(graph)
    
    # This contains a list of Nodes to start a DAG with
    nodes_to_be_process_and_test = iter(fixture['traversals'])
    
    # for each node, get its out_edges after processing to DAG 
    for at_node in nodes_to_be_process_and_test:
        dag.process(at_node)
        traversals = get_nodes_traversals(dag)
        expected = fixture['traversals'][at_node] 
        
        assert traversals == expected, "Expected Set of Traversals is different"               
    
    
    
    
    
    
    
    
    
