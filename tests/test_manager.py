import pandas as pd
from crossfilter_dataframe import (CrossFilters, TablesManager, loader_factory,
                                   table_factory)


def test_tablesmanager_can_reset():
    
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
    
    # filter the tables
    tables.get_table('Table1').filter('col1 == "a"')
    tables.get_table('Table2').filter('col2 == "item1"')
    
    tables.reset_tables()
    assert tables.get_table('Table1').collect().equals(df1)
    assert tables.get_table('Table2').collect().equals(df2)