from typing import Any, Callable, Literal, NewType

# Magic Keywords & types

ROOT = Literal['ROOT']  # string for DAG starting node

NodeCallbackFn = Callable[[str, str], Any]
