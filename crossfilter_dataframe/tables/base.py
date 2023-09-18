from __future__ import annotations

from abc import ABC, abstractmethod
from collections import namedtuple
from dataclasses import dataclass, field
from typing import Any, List, Tuple, Type, Union

TableRelation = namedtuple('TableRelation', 'table fkeys pkeys')

    

@dataclass
class Table(ABC):
    data: Type
    dwnstream_rel: List[TableRelation] = field(default_factory=list)
        

    def __call__(self, *args) -> Any:
        if args:
            self.data = args[0]

        return self.data

    @abstractmethod
    def _fkeys_table(self, fkeys):
        """return a table with only foreign keys"""
        raise NotImplementedError

    @abstractmethod
    def filter(self, *args, **kwargs):
        """filter implementation that would change the underlying table"""
        raise NotImplementedError

    @abstractmethod
    def join(self, other: Table, foreign_kys: List[str], primary_kys: List[str]):
        """inner join implementation that would filter the tables by its `_pkeys_table`"""
        raise NotImplementedError

    def set_downstreams(self, dwn_rels: List[TableRelation]):
        self.dwnstream_rel = dwn_rels

    def add_downstream(self, dwn_rel: TableRelation):
        self.dwnstream_rel.append(dwn_rel)

    def downstream_filter(self):
        """Dynamically filter all the downstream Tables

        This relies on:
        1. using the current Table foreign keys to inner join the other Table pkeys as rows filtering
        2. invoke the otherTable.downstream_filter as to chain down all downstream Tables
        """
        if not self.dwnstream_rel:
            return

        # filter join on all tables in this layer
        # before crossfiltering the children tables
        for other, fkeys, pkeys in self.dwnstream_rel:
            self.join(other, fkeys, pkeys)

        # invoke children tables to crossfilter followafter
        for other, _, _ in self.dwnstream_rel:
            other.crossfilter()


