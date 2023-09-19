from base import Table, TableRelation
from typing import Mapping


class TableFilterProtocol:
    _tables: Mapping[str, Table] = {}
    _join_keys = {}
    
    def build_downstream(self, root: str, dwnstream: str):
        # find the table from the map
        # create downstream tables for filtering
        # and pass it to DAGExecutor.callback
        
        root_table = self._tables.get(root, None)  # these are ptr back the _tables
        next_table = self._tables.get(dwnstream, None)
        
        assert all([root_table, next_table]), "some Name is not set"
        
        # get the keys     
        keys = self._join_keys.get(root).get(dwnstream)
        relations = TableRelation(table=next_table, 
                                  fkeys=keys.get('fkeys'),
                                  pkeys=keys.get('pkeys'))
        
        root_table.add_downstream(relations)
        
    
    def reset_all_dwnstreams(self):
        for table in self._tables.values():
            table.clear_downstreams()

    
    def filter_start_at(self, _key: str, *filter_fn_args, **filter_fn_kwargs):
        start_table = self._tables.get(_key)
        start_table.filter(*filter_fn_args, **filter_fn_kwargs)
        
        # once the filter chain is completed, reset all the downstreams
        self.reset_all_dwnstreams()
        

        
if __name__ == '__main__':
    from crossfilter_dataframe.tables import PandasTable
    import pandas as pd
    
    df1 = pd.DataFrame({'pkey1': [1, 2, 3, 4], 
                        'col1': 'a b c d'.split()})
    df2 = pd.DataFrame({'fkey1': [1, 1, 2, 2], 
                        'pkey2': [10, 20, 30, 40], 
                        'col2':  'item1 item2 item3 item4'.split()})    
    
    manager = TableFilterProtocol()
    manager._tables = {
        'Table1': PandasTable(df1), 
        'Table2': PandasTable(df2)
    }
    manager._join_keys = {
        'Table1': {
            'Table2': {
                'pkeys': ['pkey1'], 
                'fkeys': ['fkey1'],
            }
        }
    }
    
    manager.build_downstream('Table1', 'Table2')
    manager.filter_start_at('Table1', 'pkey1 == 1')
    
    print(manager._tables['Table1'])
    print(manager._tables['Table2'])  
        
        
        
        