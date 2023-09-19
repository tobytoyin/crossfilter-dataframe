from abc import ABC, abstractmethod

import networkx as nx

from ...types import RelationalMap


def parse_edges_tuples(parent: str, children: list):
    res = [(parent, child) for child in children]
    return res


class Loader(ABC):
    def __init__(self) -> None:
        self.network = None
        super().__init__()

    @abstractmethod
    def loader_callback(self, *args, **kwargs) -> RelationalMap:
        # implementation class can select one of the below loader function
        ...

    def load_network(self, *args, **kwargs) -> nx.Graph:
        network = self._load_relations(*args, **kwargs)
        g = nx.Graph()

        for key, childnodes in network.items():
            g.add_node(key)
            g.add_edges_from(parse_edges_tuples(key, [*childnodes]))

        return g

    def _load_relations(self, *args, **kwargs,) -> RelationalMap:
        if self.network:
            return self.network

        self.network = self.loader_callback(*args, **kwargs)
        return self.network
