from abc import ABC, abstractmethod
from inspect import getfullargspec

import networkx as nx

from crossfilter_dataframe.graphs.utils import (add_edges_from_map,
                                                invert_keys_elements)

from ...logger import logging
from ...types import FOREIGN_KEY, PRIMARY_KEY, RelationalMap


class Loader(ABC):
    def __init__(self, **kwargs) -> None:
        self._kwargs = kwargs
        
        self.network = None
        self.graph: nx.Graph = None
        self._relational_map: RelationalMap = None
        super().__init__()

    @abstractmethod
    def _loader_callback(self, *args, **kwargs) -> dict:
        # implementation class can select one of the below loader function
        ...

    def get_network(self) -> nx.Graph:
        network = self._load_network_dict()
        g = nx.Graph()

        for key, childnodes in network.items():
            g.add_node(key)
            g = add_edges_from_map(g=g, parent=key, edges_map=childnodes)

        self.graph = g
        return g

    @property
    def relational_map(self) -> RelationalMap:
        if self._relational_map:
            return self._relational_map

        r_map = nx.to_dict_of_dicts(self.graph)

        # select nodes that have the pkeys->fkeys relationship as defined
        r_map_natural = {k: v for k, v in r_map.items() if k in self.network}

        # select nodes that have the fkeys->pkeys reverse rel, and reverse the key:
        r_map_reverse = {k: v for k, v in r_map.items() if k not in self.network}
        r_map_reverse = self._invert_relational_map(r_map_reverse)

        r_map_natural.update(r_map_reverse)
        self._relational_map = r_map_natural.copy()
        return self._relational_map

    def _invert_relational_map(self, d: dict):
        # for nodes that have the fkeys->pkeys reverse rel, and reverse the key:
        # l_tab.pkeys -> r_tab.fkeys ==> r_tab.pkeys --> l_tab.fkeys
        res = {}
        invert_fn = lambda d: invert_keys_elements(d, (PRIMARY_KEY, FOREIGN_KEY))

        for l_tab, r_tabs in d.items():
            res[l_tab] = {k: invert_fn(v) for k, v in r_tabs.items()}

        return res

    def _load_network_dict(
        self,
        *args,
        **kwargs,
    ) -> dict:
        if self.network:
            return self.network
        
        # check the args in callback and pick from self.kwargs
        req_kwds = getfullargspec(self._loader_callback).args
        req_kwds = filter(lambda k: k != 'self', req_kwds)
        kwargs = { key: self._kwargs[key] for key in req_kwds  }

        self.network = self._loader_callback(**kwargs)
        return self.network
