from typing import Any, List

import networkx as nx

from ..types import ROOT, NodeCallbackFn


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

    def walk(self, start=ROOT):
        self._initial_walk_procedure()

        for node in self.walk_order:
            if node != start:
                continue

            self._call_functions_on_neightbours(node)

    def _call_functions_on_neightbours(self, node: str, skip_root=True):
        if skip_root and node == ROOT:
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
