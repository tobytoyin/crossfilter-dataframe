from __future__ import annotations

import json
from pprint import PrettyPrinter

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd

from crossfilter_dataframe.graphs.dag import DAGExecutor, DAGProcessor
from crossfilter_dataframe.graphs.loaders.loaders import JSONLoader, YAMLLoader
from crossfilter_dataframe.tables import PandasTable

loader = YAMLLoader()
G = loader.load_network('tests/examples/specs/3-1-1.yaml')

dag = DAGProcessor(G)
dag.process('Table2')


# from crossfilter_dataframe.graphs.dag.utils import get_nodes_traversals

# # for at_node in ['Table1']:
# dag.process('Table1')
# traversals = get_nodes_traversals(dag)
# print(traversals)

nx.draw(dag.dag, with_labels=True)
plt.show()


print(nx.to_dict_of_dicts(dag.dag, edge_data=True))





# df1 = pd.DataFrame({'pk1': [1, 2, 3, 4], 'col1': [1, 2, 3, 4]})
# df2 = pd.DataFrame({'pk1': [1, 2, 5, 6], 'col2': [11, 22, 33, 44]})
# df3 = pd.DataFrame({'col2': [22, 33, 44, 55], 'col3': [222, 333, 444, 555]})

# tables = {
#     'Table1': PandasTable('Table1', df1),    
#     'Table2': PandasTable('Table2', df2),
#     'Table3': PandasTable('Table3', df3),
# }

# # add entry point
# # G.add_edge('root', 'Table1')

# # do a hooping on nodes
# # for node in G.neighbors('Table1'):
# dag = DAGProcessor(G)
# processed = dag.process('Table1')



# callchains = DAGExecutor(processed)

# # def fn1(r, e):
# #     upstream_tab = tables.get(r)
# #     dwnstream_tab = tables.get(e)
    
# #     print(upstream_tab)
# #     print(dwnstream_tab)
# #     tables.get(r).add_downstream(e)
# #     print(tables.get(r).dwnstream_rel)
    

# # callchains.add_callback(fn1)


# callchains.walk()

# # print(callchains.callbacks_results)



# # # dag.

# # # # print(nx.is_directed_acyclic_graph(dag))
# # # nx.draw(dag.dag, with_labels=True)
# # # plt.show()

# # # print(processed.out_edges('Table1'))



# # # labels = {e: n.edges[e]['joinkeys'] for e in n.edges}
# # # nx.draw_networkx_edge_labels(n, pos)
# # plt.show()
