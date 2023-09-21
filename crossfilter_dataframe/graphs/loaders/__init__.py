from typing import Literal

from .loaders import DictLoader, JSONLoader, YAMLLoader

Kinds = Literal['dict', 'json', 'yaml']


def loader_factory(kind: Kinds):
    _map = {
        'dict': DictLoader, 
        'json': JSONLoader, 
        'yaml': YAMLLoader, 
    }    
    return _map.get(kind)