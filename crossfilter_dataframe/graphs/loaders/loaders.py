import json

import yaml

from ...types import RelationalMap
from .base import Loader


class JSONLoader(Loader):
    def _loader_callback(self, path: str) -> RelationalMap:
        with open(path, 'r') as f:
            return json.load(f)
    
    
class YAMLLoader(Loader):
    def _loader_callback(self, path: str) -> RelationalMap:
        with open(path, 'r') as f:
            return yaml.safe_load(f)

class DictLoader(Loader):
    def _loader_callback(self, data: dict) -> RelationalMap:
        return data
    


    
