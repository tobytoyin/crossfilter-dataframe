from crossfilter_dataframe.graphs.loaders import JSONLoader, YAMLLoader

from .examples_fixtures import diff_pkeys_fkeys, example1, many_to_one


def test_edges(example1):
    json_graph = JSONLoader().get_network(example1['paths']['json'])
    yaml_graph = YAMLLoader().get_network(example1['paths']['yaml'])
    
    expected = example1['edges']
    
    assert set(json_graph.edges) == expected, ()
    assert set(yaml_graph.edges) == expected, ()
    

def test_two_ways_keys_retrieval(many_to_one):
    loader = YAMLLoader()
    _ = loader.get_network(many_to_one['paths'])
    relational_map = loader.relational_map
    
    # (Table1) -> (Table4) get
    forward_get = relational_map.get('Table1')
    assert 'Table4' in forward_get
    assert forward_get['Table4']['pkeys'] == ['k1']
    
    # (Table4) --> (Table1) get 
    reverse_get = relational_map.get('Table4')
    assert 'Table1' in reverse_get
    assert reverse_get['Table1']['pkeys'] == ['k1']    
    
    
def test_two_ways_pkeys_fkeys_would_flip(diff_pkeys_fkeys):
    """scenario where pkeys & fkeys would flip when the keyed table is reversed"""
    loader = YAMLLoader()
    _ = loader.get_network(diff_pkeys_fkeys)
    relational_map = loader.relational_map
    
    # (Table1) -> (Table2) get
    forward_get = relational_map.get('Table1')
    assert forward_get['Table2']['pkeys'] == ['col_1', 'col_2']
    assert forward_get['Table2']['fkeys'] == ['col_1_id', 'col_2_id']
    
    # (Table2) --> (Table1) get 
    reverse_get = relational_map.get('Table2')
    assert reverse_get['Table1']['pkeys'] == ['col_1_id', 'col_2_id'] 
    assert reverse_get['Table1']['fkeys'] == ['col_1', 'col_2']
        
    
    
    
    
    
    
