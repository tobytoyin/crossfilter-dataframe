import pandas as pd

from base import Table
from functools import partial


class PandasTable(Table):
    _table: pd.DataFrame

    def join_keys_table(self, fkeys):
        return self.data[fkeys].drop_duplicates()
    
    def filter_fn(self, dataframe, query_string):
        return dataframe.query(query_string)
      
    def join_fn(self, target, other, pkeys, fkeys):
        return target.merge(other, left_on=pkeys, right_on=fkeys, how='inner')
    

        


if __name__ == '__main__':
#     df1 = pd.DataFrame({'pk1': [1, 2, 3, 4], 'col1': [1, 2, 3, 4]})
#     df2 = pd.DataFrame({'pk1': [1, 2, 5, 6], 'col2': [11, 22, 33, 44]})
#     df3 = pd.DataFrame({'col2': [11, 22, 33, 44, 55], 'col3': [111, 222, 333, 444, 555]})
# # 
#     table1 = PandasTable(df1)
#     table2 = PandasTable(df2)
#     table3 = PandasTable(df3)
# # 
#     table1.set_downstreams([(table2, ['pk1'], ['pk1'])])
#     table2.set_downstreams([(table3, ['col2'], ['col2'])])
# # 
#     table1.filter('pk1 == 1')
#     table1.downstream_filter()
#     # 
# # 
#     print(table1())
#     print(table2())
#     print(table3())
    
    df2 = pd.DataFrame({
        'k1': [1, 2, 3, 4, 1]
    })
    
    df3 = pd.DataFrame({
       'k2': [11, 22, 33, 44, 55] 
    })
    
    df5 = pd.DataFrame({
        'k1': [1, 1, 2, 2,], 
        'k2': [11, 22, 33, 44], 
        'k3': [111, 222, 333, 444]
    })

    table2 = PandasTable(df2)
    table3 = PandasTable(df3)
    table5 = PandasTable(df5)

    table2.set_downstreams([(table5, ['k1'], ['k1'])])
    table5.set_downstreams([(table3, ['k2'], ['k2'] )])
    table2.filter('k1 == 1')    
    
    print(table2.collect())
    print(table5.collect())
    print(table3.collect())
    
    print('-' * 20)
    

    table2.clear_downstreams()
    table3.set_downstreams([(table5, ['k2'], ['k2'] )])    
    table5.set_downstreams([(table2, ['k1'], ['k1'])])
    table3.filter('k2 == 22')    
    
    print(table2.data)
    print(table3.data)
    print(table5.data)   
    
    
