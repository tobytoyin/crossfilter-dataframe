from abc import ABC, abstractmethod

import networkx as nx


def parse_edges_tuples(parent: str, children: list):
    res = [(parent, child) for child in children]
    return res


class Loader(ABC):
    def __init__(self) -> None:
        self.network = None
        super().__init__()

    @abstractmethod
    def loader_callback(self, path) -> dict:
        # implementation class can select one of the below loader function
        NotImplementedError

    def load_network(self, path: str) -> nx.Graph:
        network = self.load_relations(path)
        g = nx.Graph()

        for key, childnodes in network.items():
            # add nodes
            g.add_node(key)
            g.add_edges_from(parse_edges_tuples(key, [*childnodes]))

        return g

    def load_relations(self, path: str) -> dict:
        if self.network:
            return self.network

        self.network = self.loader_callback(path)
        return self.network
