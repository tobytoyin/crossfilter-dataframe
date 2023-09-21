from .loaders import DictLoader, JSONLoader, YAMLLoader


def loader_factory(kind: str):
    _map = {
        'dict': DictLoader, 
        'json': JSONLoader, 
        'yaml': YAMLLoader, 
    }    
    return _map.get(kind)