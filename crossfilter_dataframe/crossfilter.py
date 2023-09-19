from typing import Mapping

from tables import Table
from tables.types import TableRelation


class TableFilterProtocol:
    tables: Mapping[str, Table] = {}
    join_keys = {}
    
    def build_downstream(self, root: str, dwnstream: str):
        # find the table from the map
        # create downstream tables for filtering
        # and pass it to DAGExecutor.callback
        
        root_table = self.tables.get(root, None)  # these are ptr back the _tables
        next_table = self.tables.get(dwnstream, None)
        
        assert all([root_table, next_table]), "some Name is not set"
        
        # get the keys     
        keys = self.join_keys.get(root).get(dwnstream)
        relations = TableRelation(table=next_table, 
                                  fkeys=keys.get('fkeys'),
                                  pkeys=keys.get('pkeys'))
        
        root_table.add_downstream(relations)
        
    
    def reset_all_dwnstreams(self):
        for table in self.tables.values():
            table.clear_downstreams()

    
    def filter_start_at(self, _key: str, *filter_fn_args, **filter_fn_kwargs):
        start_table = self.tables.get(_key)
        start_table.filter(*filter_fn_args, **filter_fn_kwargs)
        
        # once the filter chain is completed, reset all the downstreams
        self.reset_all_dwnstreams()
        

        
if __name__ == '__main__':
    pass