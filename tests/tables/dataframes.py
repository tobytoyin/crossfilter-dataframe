import pandas as pd
import pytest


@pytest.fixture
def dataframe_set1():
    # test linear filtering
    # df1 --> df2 --> df3
    df1 = pd.DataFrame({'pk1': [1, 2, 3, 4], 'col1': [1, 2, 3, 4]})
    df2 = pd.DataFrame({'pk1': [1, 2, 5, 6], 'col2': ['a', 'b', 'c', 'd']})
    df3 = pd.DataFrame({'col2': ['b', 'c', 'e', 'f'], 'col3': [222, 333, 444, 555]})
    
    