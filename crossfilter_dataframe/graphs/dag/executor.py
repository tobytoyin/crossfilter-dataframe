from typing import Any, List

import networkx as nx

from ...logger import logging
from ...types import ROOT_NODE, NodeCallbackFn


class DAGExecutor:
    """DAGCallChain walks along the nodes of a DAG and invoke callbacks function at each walk"""

    def __init__(self, dag: nx.DiGraph) -> None:
        self.dag = dag
        self._callbacks: List[NodeCallbackFn] = []
        self.callbacks_results: List[Any] = None
        self.walk_order = nx.topological_sort(self.dag)

    def _initial_walk_procedure(self):
        """Steps to do when the walk first initially starts"""
        self.callbacks_results = []

    def walk(self, start=ROOT_NODE):
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

        Args:
            node (str): string value of the current node in the DAG
            skip_root (bool, optional): _description_. Defaults to True.
        """
        if node == ROOT_NODE:
            return

        for _, next_node in self.dag.out_edges(node):
            print(f'CALL FUNCTION AT {node}\t- FN({node}, {next_node})')

            # call the chain of callback functions
            self.callbacks_results.append([fn(node, next_node) for fn in self._callbacks])

    def add_callback(self, fn: NodeCallbackFn) -> None:
        """Setup Callback functions at each node traversal along the DAG

        (ROOT) --> (NEXT) --> < CALL FUNCTION >

        Args:
            fn (DAGCallbackFunction): Function with signature fn(ROOT, NEXT)
        """
        self._callbacks.append(fn)
