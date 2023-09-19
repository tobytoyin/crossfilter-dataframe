# from crossfilter_dataframe.tables import PandasTable
# import pandas as pd

# def test_simple_protocol():
#     df1 = pd.DataFrame({'pkey1': [1, 2, 3, 4], 
#                         'col1': 'a b c d'.split()})
#     df2 = pd.DataFrame({'fkey1': [1, 1, 2, 2], 
#                         'pkey2': [10, 20, 30, 40], 
#                         'col2':  'item1 item2 item3 item4'.split()})    

# manager = TableFilterProtocol()
# manager._tables = {
#     'Table1': PandasTable(df1), 
#     'Table2': PandasTable(df2)
# }
# manager._join_keys = {
#     'Table1': {
#         'Table2': {
#             'pkeys': ['pkey1'], 
#             'fkeys': ['fkey1'],
#         }
#     }
# }

# manager.build_downstream('Table1', 'Table2')
# manager.filter_start_at('Table1', 'pkey1 == 1')

# print(manager._tables['Table1'])
# print(manager._tables['Table2'])  