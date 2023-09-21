from ..types import Data
from .base import Table
from .pandas_dataframe import PandasTable


def table_factory(table_type: str, data: Data) -> Table:
    _map = {
        'pandas': PandasTable
    }
    
    return _map.get(table_type)(data)
