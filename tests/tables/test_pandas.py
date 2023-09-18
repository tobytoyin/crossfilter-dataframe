import pandas as pd
from crossfilter_dataframe.tables import PandasTable

def test_1_way_1_table_filter():
    # direct key map relational key filtering
    # (filter) -> [df1] -> [df2]
    df1 = pd.DataFrame({'pkey1': [1, 2, 3, 4], 
                        'col1': 'a b c d'.split()})
    df2 = pd.DataFrame({'fkey1': [1, 1, 2, 2], 
                        'col2':  'item1 item2 item3 item4'.split()})
    
    filter_df1 = pd.DataFrame({'pkey1': [1], 
                               'col1': 'a'.split()})
    filter_df2 = pd.DataFrame({'fkey1': [1, 1], 
                               'col2':  'item1 item2'.split()})                               
    
    table1 = PandasTable(df1)
    table2 = PandasTable(df2)
    
    table1.set_downstreams([(table2, ['pkey1'], ['fkey1'])])
    table1.filter('pkey1 == 1')
    
    assert table1.collect().equals(filter_df1)
    assert table2.collect().equals(filter_df2)


def test_1_way_2_table_filter():
    # cross relational key filtering
    # (filter) -> [df1] -> [df2] -> [df3]
    df1 = pd.DataFrame({'pkey1': [1, 2, 3, 4], 
                        'col1': 'a b c d'.split()})
    df2 = pd.DataFrame({'fkey1': [1, 1, 2, 2], 
                        'pkey2': [10, 20, 30, 40], 
                        'col2':  'item1 item2 item3 item4'.split()})
    df3 = pd.DataFrame({'fkey2': [10, 11, 20, 21, 30, 31], 
                        'col2':  '10 11 20 21 30 31'.split()})                        
    
    filter_df3 = pd.DataFrame({'fkey2': [10, 20], 
                                'col2':  '10 20'.split()})                                    
    
    table1 = PandasTable(df1)
    table2 = PandasTable(df2)
    table3 = PandasTable(df3)
    
    table1.set_downstreams([(table2, ['pkey1'], ['fkey1'])])
    table2.set_downstreams([(table3, ['pkey2'], ['fkey2'])])
    table1.filter('pkey1 == 1')
    
    assert table3.collect().equals(filter_df3)