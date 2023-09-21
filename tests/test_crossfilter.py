import pandas as pd

from crossfilter_dataframe import (CrossFilters, TablesManager, loader_factory,
                                   table_factory)


def test_simple_protocol():
    
    df1 = pd.DataFrame({'pkey1': [1, 2, 3, 4], 
                        'col1': 'a b c d'.split()})
    df2 = pd.DataFrame({'fkey1': [1, 1, 2, 2], 
                        'pkey2': [10, 20, 30, 40], 
                        'col2':  'item1 item2 item3 item4'.split()})   
    
    # relations
    relations = {
        'Table1': {
            'Table2': {
                'pkeys': ['pkey1'], 
                'fkeys': ['fkey1'],
            }
        }
    }
    
    # create graph based on relations 
    loader = loader_factory(kind='dict')(data=relations)
        
    # setup
    tables = TablesManager(specs_loader=loader)
    tables.add_table('Table1', table_factory('pandas', df1)) 
    tables.add_table('Table2', table_factory('pandas', df2)) 
    
    # prepare for filter
    filters = CrossFilters(tables_manager=tables)
    filters.filter('Table2', 'fkey1 == 1')
    
    # test
    assert 
    
    print(filters.tables_manager.tables['Table1'])
    print(filters.tables_manager.tables['Table2'])