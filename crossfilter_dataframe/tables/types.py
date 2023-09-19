from collections import namedtuple
from typing import TypeVar

TableRelation = namedtuple('TableRelation', 'table fkeys pkeys')
Data = TypeVar('Data')  # Generic for external library data structure