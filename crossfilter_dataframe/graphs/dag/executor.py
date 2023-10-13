from typing import Any, List

import networkx as nx

from ...logger import logging
from ...types import ROOT_NODE, NodeCallbackFn


class DAGExecutor:
    """Walks a DiGraph and apply function to the paired Nodes 

    DAGExecutor is purposed to apply custom function to the paired Nodes.\
    This involves: 
    1. walking along the DiGraph at topological order 
    2. at each walk, invokes a list of callback functions by \
        passing the current node and the neighbour node to the function
    3. function results are append to the results return list 
    4. walk until the Digraph is completed
    
    """
    
    def __init__(self, dag: nx.DiGraph) -> None:
        self.dag = dag
        self._callbacks: List[NodeCallbackFn] = []
        self.callbacks_results: List[Any] = []
        self.walk_order = nx.topological_sort(self.dag)

    def _initial_walk_procedure(self):
        """Steps to do when the walk first initially starts"""
        self.callbacks_results = []

    def walk(self):
        """Walk the DAG, execute all callbacks for each node hops

        Args:
            start (_type_, optional): _description_. Defaults to ROOT_NODE.
        """
        self._initial_walk_procedure()
        for node in self.walk_order:
            self._call_functions_on_neightbours(node)

    def _call_functions_on_neightbours(self, node: str):
        """Invoke all the NodeCallbackFn functions given the context of \
            the string value of the current node and downstream node
        
        (ROOT) --> (NEXT) --> < CALL FUNCTION >

        Args:
            node (str): string value of the current node in the DAG
            skip_root (bool, optional): _description_. Defaults to True.
        """
        if node == ROOT_NODE:
            return

        for _, next_node in self.dag.out_edges(node):
            logging.debug(f'CALL FUNCTION AT {node}\t- FN({node}, {next_node})')

            # call the chain of callback functions
            self.callbacks_results.append([fn(node, next_node) for fn in self._callbacks])

    def add_callback(self, fn: NodeCallbackFn) -> None:
        """Setup Callback functions at each node traversal along the DAG

        Args:
            fn (DAGCallbackFunction): Function with signature fn(ROOT, NEXT)
        """
        self._callbacks.append(fn)
