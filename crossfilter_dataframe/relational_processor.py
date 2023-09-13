import json

import matplotlib.pyplot as plt
import networkx as nx
from network_loader import JSONLoader

n = JSONLoader.load_network('/Users/tobiasto/Projects/crossfilter-dataframe/tests/examples/example1.json')
print(n)



