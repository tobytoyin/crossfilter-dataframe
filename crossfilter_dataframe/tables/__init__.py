from ..types import Data, Literal
from .base import Table
from .pandas_dataframe import PandasTable

Keys = Literal['pandas']

def table_factory(table_type: Keys, data: Data) -> Table:
    _map = {
        'pandas': PandasTable
    }
    
    return _map.get(table_type)(data)
