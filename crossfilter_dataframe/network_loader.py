import json

import matplotlib.pyplot as plt
import networkx as nx


class Loader:
    def set_edge_tuple(parent: str, child: dict):
        print(child)
        childkey, joinkeys = child.popitem()
        return {
            (parent, childkey): {'joinkeys': joinkeys}
        }
        
        
        
    


class JSONLoader(Loader):
    def validator(self):
        ...

    @staticmethod
    def json_to_dict(path) -> dict:
        assert 'json' in path.split('.')[-1]

        with open(path, 'r') as f:
            return json.load(f)

    @classmethod
    def load_network(self, path) -> nx.Graph:
        network = self.json_to_dict(path)

        g = nx.Graph()

        for key, childnodes in network.items():
            # add nodes
            g.add_node(key)
            g.add_nodes_from([*childnodes])

            # add relations
            edges = [self.set_edge_tuple(key, childkey) for childkey in childnodes.]
            g.set_edge_attributes(edges)

        return g
