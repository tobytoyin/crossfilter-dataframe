from __future__ import annotations

from abc import ABC, abstractmethod
from collections import namedtuple
from dataclasses import dataclass
from typing import Any, List, Tuple, Type, Union

DownstreamTable = namedtuple('DownstreamTable', 'table fkeys pkeys')


@dataclass
class Table(ABC):
    data: Type
    dwnstream_rel: List[DownstreamTable] = None
        
    def __call__(self, *args) -> Any:
        if args:
            self.data = args[0]
            
        return self.data
    
    def set_downstreams(self, dwn_rel: List[DownstreamTable]):
        self.dwnstream_rel = dwn_rel
        
    @abstractmethod
    def _pkeys_table(self, fkeys):
        """return a table with only foreign keys"""
        ...
        
    @abstractmethod
    def filter(self):
        """filter implementation that would change the underlying table"""
        ...
        
        
    @abstractmethod
    def join(self, 
             other: Table, 
             foreign_kys: List[str],
             primary_kys: List[str]):
        ...
        
        
    def crossfilter(self):
        if not self.dwnstream_rel:
            return 
        
        # filter join on all tables in this layer
        # before crossfiltering the children tables
        for other, fkeys, pkeys in self.dwnstream_rel:
            self.join(other, fkeys, pkeys)
        
        # invoke children tables to crossfilter followafter
        for other, _, _ in self.dwnstream_rel:
            other.crossfilter()