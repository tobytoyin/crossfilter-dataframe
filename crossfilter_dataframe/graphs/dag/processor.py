import networkx as nx

from ...logger import logging
from ..types import ROOT


class DAGProcessor:
    """Turns Undirected Relational Graph into a DAG at a defined starting root
    
    Networkx provides a way to convert any undirected relational graph into a \
        bi-directed Graph. To further converts this into a uni-directed DAG, DAGProcessor \
        takes a walk along all the NODES and turn convert bi-directional into uni-directional
    """

    def __init__(self, graph: nx.Graph) -> None:
        self.graph = graph.copy()
        self.dag = None
        self.upstream_parents = None
        self.prev_completed_upstream = None
        
    @staticmethod
    def set_oneway_direction(graph: nx.DiGraph, root, upstream_parents: list = None):
        """Given a DiGraph, converts bi-directional nodes into uni-directional.
        
        - [ (ROOT) <-> (NODE_N) ] converts all into [ (ROOT) --> (NODE_N) ]
        - [ (ROOT) <-- (PARENT) ] are presevered given a list of `upstream_parents`

        Args:
            graph (nx.DiGraph): nx.Directed Graph
            root (any): the ROOT node which the uni-directional starts
            parent_in_edges (list, optional): list of NODEs that belongs to the DAG upstream NODEs of ROOT. \
                These NODEs are preserved as these incoming directions are intended. Defaults to None.
        """
        in_edges = list(graph.in_edges(root))

        # gather all nodes that are directed into root
        # and remove them as to forece uni-direction
        for in_node, _ in in_edges:
            # ignore if in_edges is part of the parents edges
            if in_node in upstream_parents:
                logging.debug(f"ignore direction (upstream parents): {upstream_parents}")
                continue

            graph.remove_edge(in_node, root)

    def hop_neighbors(self, root: str):
        # Start walking the bi-directed DAG from ROOT
        logging.debug(f"AT {root}")

        # NODES that has already been fixed (uni-directed) are ignored
        walkable_nodes = set(self.dag.neighbors(root)) - set(self.upstream_parents)
        logging.debug(f"walkable nodes: {walkable_nodes}")

        # fixes bi-directed pairs to uni-directed pairs to create DAG
        # e.g., (ROOT) <-> (NODE1) converts into (ROOT) --> (NODE1)
        for node in walkable_nodes:
            logging.debug(f"setting unidirection from {root} -> {node}")
            self.set_oneway_direction(self.dag, root, self.upstream_parents)

        self.upstream_parents.append(root)
        for node in walkable_nodes:
            self.hop_neighbors(node)

    def process(self, start_root) -> nx.DiGraph:
        logging.debug(f"\r----- CREATING DAG STARTING AT {start_root} -----")
        # setup: refresh dag with at start_root & reset completed upstream
        self.dag = self.graph.to_directed().copy()
        self.upstream_parents = []
        
        self.hop_neighbors(start_root)
        
        # add root entry
        self.dag.add_edge(ROOT, start_root)
        
        return self.dag
