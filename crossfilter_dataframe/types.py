from collections import namedtuple
from typing import Any, Callable, Dict, List, Literal, TypeVar

# Constants
PRIMARY_KEY = 'pkeys'
FOREIGN_KEY = 'fkeys'
ROOT_NODE = 'ROOT'

# Module shared types
Keys = List[str]
JoinKeys = Dict[Literal['pkeys', 'fkeys'], Keys]
Relational  = Dict[str, JoinKeys]
RelationalMap = Dict[str, Relational]
NodeCallbackFn = Callable[[str, str], Any]
TableRelation = namedtuple('TableRelation', 'table fkeys pkeys')

# Generics
Data = TypeVar('Data')  # Generic for external library data structure

