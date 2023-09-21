import pandas as pd

from .base import Table


class PandasTable(Table):
    _table: pd.DataFrame

    def distinct_index_table(self, keys):
        return self.data[keys].drop_duplicates()

    def filter_fn(self, dataframe, query_string):
        return dataframe.query(query_string)

    def join_fn(self, target, other, pkeys, fkeys):
        joined = target.merge(other, left_on=pkeys, right_on=fkeys, how='inner')
        return joined[target.columns]


if __name__ == '__main__':
    pass
# # #
# #     print(table1())
# #     print(table2())
# #     print(table3())

#     df2 = pd.DataFrame({
#         'k1': [1, 2, 3, 4, 1]
#     })

#     df3 = pd.DataFrame({
#        'k2': [11, 22, 33, 44, 55]
#     })

#     df5 = pd.DataFrame({
#         'k1': [1, 1, 2, 2,],
#         'k2': [11, 22, 33, 44],
#         'k3': [111, 222, 333, 444]
#     })

#     table2 = PandasTable(df2)
#     table3 = PandasTable(df3)
#     table5 = PandasTable(df5)

#     table2.set_downstreams([(table5, ['k1'], ['k1'])])
#     table5.set_downstreams([(table3, ['k2'], ['k2'] )])
#     table2.filter('k1 == 1')

#     print(table2.data)
#     print(table5.data)
#     print(table3.data)

#     print('-' * 20)


#     table2.clear_downstreams()
#     table3.set_downstreams([(table5, ['k2'], ['k2'] )])
#     table5.set_downstreams([(table2, ['k1'], ['k1'])])
#     table3.filter('k2 == 22')

#     print(table2.data)
#     print(table3.data)
#     print(table5.data)
