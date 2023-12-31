from typing import Mapping, Optional

from crossfilter_dataframe.graphs.loaders.base import Loader
from crossfilter_dataframe.tables.base import Table

from .logger import logging
from .types import FOREIGN_KEY, PRIMARY_KEY, Data, RelationalMap, TableRelation


class TablesManager:
    """class contains methods to manage map of tables and relationships"""

    def __init__(
        self,
        specs_loader: Loader,
        tables: Optional[Mapping[str, Table]] = None,
    ) -> None:
        self.loader = specs_loader
        self.tables_graph = self.loader.get_network()
        self.tables = tables if tables else {}
        self.join_keys = self.loader.relational_map

    @property
    def relational_map(self) -> RelationalMap:
        return self.loader.relational_map

    def add_table(self, _key: str, table: Table):
        self.tables[_key] = table

    def get_table(self, _key: str) -> Table:
        return self.tables[_key]
    
    def collect(self, _key: str) -> Data:
        """return the materialised data"""
        return self.get_table(_key).collect()

    def reset_tables(self):
        """reset all tables back to initial state"""
        for table in self.tables.values():
            table.reset()
            
    def reset(self, _key: str):
        """reset a particular table"""
        self.get_table(_key).reset()

    def reset_all_dwnstreams(self):
        for table in self.tables.values():
            table.clear_downstreams()

    def build_downstream(self, root: str, dwnstream: str):
        logging.debug(f"setting downstream ({root}) -> ({dwnstream})")
        # find the table from the map
        # create downstream tables for filtering
        # and pass it to DAGExecutor.callback

        root_table = self.tables[root]  # these are ptr back the _tables
        next_table = self.tables[dwnstream]

        assert all([root_table, next_table]), "some Name is not set"

        # get the keys
        keys = self.join_keys[root][dwnstream]
        relations = TableRelation(table=next_table, fkeys=keys[FOREIGN_KEY], pkeys=keys[PRIMARY_KEY])
        root_table.add_downstream(relations)
