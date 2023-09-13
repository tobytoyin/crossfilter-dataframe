import pandas as pd
from base import Table


class PandasTable(Table):
    _table: pd.DataFrame

    def _pkeys_table(self, fkeys):
        return self.data[fkeys]
    
    def filter(self, query_string):
        self(self.data.query(query_string))

    def join(self, other: Table, fkeys, pkeys):
        _other: pd.DataFrame = other.data
        result = _other.merge(
            self._pkeys_table(fkeys),
            left_on=fkeys,
            right_on=pkeys,
            how='inner',
        )
        other(result)


if __name__ == '__main__':
    df1 = pd.DataFrame({'pk1': [1, 2, 3, 4], 'col1': [1, 2, 3, 4]})
    df2 = pd.DataFrame({'pk1': [1, 2, 5, 6], 'col2': [11, 22, 33, 44]})
    df3 = pd.DataFrame({'col2': [22, 33, 44, 55], 'col3': [222, 333, 444, 555]})

    table1 = PandasTable(df1)
    table2 = PandasTable(df2)
    table3 = PandasTable(df3)

    table1.set_downstreams([(table2, ['pk1'], ['pk1'])])
    table2.set_downstreams([(table3, ['col2'], ['col2'])])

    table1.filter('pk1 == 2')
    table1.crossfilter()

    print(table2.data)
    print(table3.data)
