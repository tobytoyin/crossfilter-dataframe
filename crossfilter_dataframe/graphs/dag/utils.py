def get_nodes_traversals(dag):
    traversals = set()
    
    for node in dag.dag.nodes:
        out_edges = list(dag.dag.out_edges(node))
        traversals.update(out_edges)

    return traversals