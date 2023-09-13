from __future__ import annotations

from collections import namedtuple
from dataclasses import dataclass
from typing import Any, List, Tuple, Type, Union

DownstreamTable = namedtuple('DownstreamTable', 'table pkeys')


@dataclass
class Table:
    data: Type
    dwnstream_rel: List[DownstreamTable] = None
        
    def __call__(self, *args) -> Any:
        if args:
            self.data = args[0]
            
        return self.data
    
    def set_downstream_relational(self, dwn_rel: list):
        self.dwnstream_rel = dwn_rel
        
    def filter(self):
        """filter implementation that would change the underlying table"""
        ...
        
        
    def join(self, other: Table, pkeys: Union[str, Tuple[str, str]]):
        ...
        
        
    def crossfilter(self):
        if not self.dwnstream_rel:
            return 
        
        # filter join on all tables in this layer
        # before crossfiltering the children tables
        for other, pkeys in self.dwnstream_rel:
            self.join(other, pkeys)
        
        # invoke children tables to crossfilter followafter
        for other, _ in self.dwnstream_rel:
            other.crossfilter()