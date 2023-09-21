import networkx as nx


def get_nodes_traversals(dag):
    traversals = set()
    
    for node in dag.dag.nodes:
        out_edges = list(dag.dag.out_edges(node))
        traversals.update(out_edges)

    return traversals

    
def add_edges_from_map(g: nx.Graph, parent: str, edges_map: dict):
    graph = g.copy()
    
    for node, edge_data in edges_map.items():
        graph.add_edge(parent, node, **edge_data)
        
    return graph
        

def invert_keys_elements(d: dict, kpairs: tuple) -> dict:
    """invert the keys of a dictionary
    
    before = { 'key1': [1, 2, 3], 'key2': [6, 7, 8] }
    after  = { 'key2': [1, 2, 3], 'key1': [6, 7, 8] }

    Args:
        d (dict): target dict
        kpairs (tuple): ( target_key, reverse_key )
    """
    res = d.copy()
    target, reverse = kpairs    
    _map = {
        target: d[reverse].copy(),
        reverse: d[target].copy(),
    }
    res.update(**_map)
    return res
    
        