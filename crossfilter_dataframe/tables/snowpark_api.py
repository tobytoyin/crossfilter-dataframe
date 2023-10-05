import pandas as pd

from .base import Table


class SnowparkTable(Table):
    def distinct_index_table(self, keys):
        return self.data[keys].drop_duplicates()

    def filter_fn(self, dataframe, query_string):
        return dataframe.query(query_string)

    def join_fn(self, target, other, pkeys, fkeys):
        joined = target.merge(other, left_on=pkeys, right_on=fkeys, how='inner')
        return joined[target.columns]


if __name__ == '__main__':
    df1 = 
    
    # sp = SnowparkTable()

