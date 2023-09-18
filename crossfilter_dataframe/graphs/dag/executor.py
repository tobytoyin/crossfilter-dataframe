from typing import Any, Callable, List

import networkx as nx

NodesCallback = Callable[[str, str], Any]


class DAGExecutor:
    """DAGCallChain walks along the nodes of a DAG and invoke callbacks function at each walk"""

    def __init__(self, dag: nx.DiGraph) -> None:
        self.dag = dag
        self._callbacks: List[NodesCallback] = []
        self.callbacks_results: List[Any] = None

    def _initial_walk_procedure(self):
        """Steps to do when the walk first initially starts"""
        self.callbacks_results = []

    def walk(self, start='ROOT'):
        # sort the walk order
        walk_order = nx.topological_sort(self.dag)
        self._initial_walk_procedure()

        for node in walk_order:
            if node != start:
                continue

            self._call_functions_on_neightbours(node)

    def _call_functions_on_neightbours(self, root: str):
        for _, next_node in self.dag.out_edges(root):
            print(f'CALL FUNCTION AT {root}\t- FN({root}, {next_node})')

            # call the chain of callback functions
            self.callbacks_results.append([fn(root, next_node) for fn in self._callbacks])

    def add_callback(self, fn: NodesCallback) -> None:
        """Setup Callback functions at each node traversal along the DAG

        (ROOT) --> (NEXT) --> < CALL FUNCTION >

        Args:
            fn (DAGCallbackFunction): Function with signature fn(ROOT, NEXT)
        """
        self._callbacks.append(fn)
