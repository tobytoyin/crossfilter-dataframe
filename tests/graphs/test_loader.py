from crossfilter_dataframe.graphs.loaders import JSONLoader, YAMLLoader

from .examples_fixtures import *


def test_edges(example1):
    json_graph = JSONLoader().load_network(example1['paths']['json'])
    yaml_graph = YAMLLoader().load_network(example1['paths']['yaml'])
    
    expected = example1['edges']
    
    assert set(json_graph.edges) == expected, ()
    assert set(yaml_graph.edges) == expected, ()
    
    
    
    
    
