import pandas as pd
from crossfilter_dataframe import CrossFilters
from crossfilter_dataframe.graphs.loaders import DictLoader
from crossfilter_dataframe.manager import TablesManager
from crossfilter_dataframe.tables import PandasTable


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
    loader = DictLoader(data=relations)
        
    # setup
    tables = TablesManager(specs_loader=loader)
    tables.add_table('Table1', PandasTable(df1)) 
    tables.add_table('Table2', PandasTable(df2)) 
    
    # prepare for filter
    filters = CrossFilters(tables_manager=tables)
    filters.filter('Table2', 'fkey1 == 1')
    # filters.filter('Table1', 'pkey1 == 1')
    
    print(filters.tables_manager.tables['Table1'])
    print(filters.tables_manager.tables['Table2'])
    
    
    



# manager.build_downstream('Table1', 'Table2')
# manager.filter_start_at('Table1', 'pkey1 == 1')

# print(manager._tables['Table1'])
# print(manager._tables['Table2'])  
# print(manager._tables['Table2'])  
