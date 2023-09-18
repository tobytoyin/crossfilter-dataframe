from __future__ import annotations

from abc import ABC, ABCMeta, abstractmethod
from collections import namedtuple
from dataclasses import dataclass, field
from typing import Any, List, Tuple, Type, Union, Callable
from functools import partial

TableRelation = namedtuple('TableRelation', 'table fkeys pkeys')


@dataclass
class TableAdapter(metaclass=ABCMeta):
    """Adapter to translate the adaptee module functions as an interface for `Table`

    """
    data: Type
    dwnstream_rel: List[TableRelation] = field(default_factory=list)
    
    def __call__(self, *args) -> Any:
        """.data can be lazily evaluated through session. __call__ would collect data"""
        if args:
            self.data = args[0]

        return self.data

    @abstractmethod
    def join_keys_table(self, keys):
        """return a table with only foreign keys"""
        raise NotImplementedError

    @abstractmethod
    def filter_fn(self, data: Any, *args, **kwargs):
        """Abstract filter method that would faciliate a filter on the Table's data

        Args:
            data (Any): the data structure of adpatee module (e.g., pd.DataFrame)
            *args, **kwargs: the filter args, kwargs used by adaptee module

        Raises:
            NotImplementedError:
        """
        raise NotImplementedError

    @abstractmethod
    def join_fn(self, /, 
                left: Any, right: Any, 
                l_keys: List[str], r_keys: List[str], ):
        """Abstract join implementation that would faciliate a join between `left` and `right`

        Args:
            left (Any): Left table of the implemented data structured
            right (Any): Right table of the implemeted data structured
            l_keys (List[str]): Join keys (pkeys) of the left table
            r_keys (List[str]): Join keys (fkeys) of the right table

        Raises:
            NotImplementedError: implementation of join function required
        """
        raise NotImplementedError    


@dataclass
class Table(TableAdapter):
    def join(self, other: Table, pkeys: List[str], fkeys: List[str]):
        """A two-way join process to update both tables
        - first right join left, to filter & update right table
        - then left join right, to filter & update left table

        Args:
            other (Table): _description_
            pkeys (List[str]): list of public keys of the left table
            fkeys (List[str]): list of foreign keys of the right table
        """
        self.forward_relational_join(self, other, pkeys, fkeys)
        
    
    def forward_relational_join(self, /,
                                target: Table, other: Table, 
                                pkeys: List[str], fkeys: List[str]):
        """Forward relatonal join, other.join(self) as to update the data in `other`

        Args:
            target (Table): _description_
            other (Table): _description_
            fkeys (_type_): _description_
            pkeys (_type_): _description_
        """
        updated_other = self.join_fn(other.data, target.join_keys_table(pkeys), fkeys, pkeys)
        other(updated_other)  # update the other table with new data
        
        
    def backward_relational_join(self, /, 
                                target: Table, other: Table, 
                                fkeys, pkeys):
        """Backward relatonal join, self.join(other) as to update the data in `self`

        Args:
            target (Table): _description_
            other (Table): _description_
            fkeys (_type_): _description_
            pkeys (_type_): _description_
        """
        updated_self = self.join_fn(target.data, other.join_keys_table(fkeys), pkeys, fkeys)
        target(updated_self)  # update the self table with new data
        
    def clear_downstreams(self):
        self.dwnstream_rel = []

    def set_downstreams(self, dwn_rels: List[TableRelation]):
        self.dwnstream_rel = dwn_rels

    def add_downstream(self, dwn_rel: TableRelation):
        self.dwnstream_rel.append(dwn_rel)

    def filter(self, *args, **kwargs):
        if not self.dwnstream_rel:
            print("[WARNING] There are no downstream tables, have you forgotten to set_downstreams?")
            
        self.filter_fn(self.data, *args, **kwargs)
        self.downstream_filter()

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
            other.downstream_filter()

    def collect(self) -> Any:
        """Return the materialised data of the Table"""
        return self()

