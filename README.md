# crossfilter-dataframe

## Use it locally
```shell
poetry shell
poetry install
```

## Build from source
```shell
# at project root
poetry build 
source .venv/bin/activate  # start any virtual env
pip3 install ./dist/<package>.whl
```

## Usage
`crossfilter-dataframe` aims to provide a simplified interface to filter down relational dataframes. For example:

```python
from crossfilter_dataframe import CrossFilters
from crossfilter_dataframe.graphs.loaders import DictLoader
from crossfilter_dataframe.tables import PandasTable, TablesManager

# setup dataframes
df1 = pd.DataFrame({'pkey1': [1, 2, 3, 4], 'col1': 'a b c d'.split()})
df2 = pd.DataFrame({'fkey1': [1, 1, 2, 2], 'pkey2': [10, 20, 30, 40], 'col2': 'item1 item2 item3 item4'.split()})   

# define how dataframes/ tables are related
relations = {
    'Table1': {
        'Table2': {
            'pkeys': ['pkey1'], 
            'fkeys': ['fkey1'],
        }
    }
}

# create graph based on relations 
loader = DictLoader()
graph = loader.get_network(relations)
relations_map = loader.relational_map
    
# setup
tables = TablesManager()
tables.add_table('Table1', PandasTable(df1)) 
tables.add_table('Table2', PandasTable(df2)) 
tables.set_relations(relation_map=relations_map)

# prepare for filter
filters = CrossFilters(tables_manager=tables, relational_graph=graph)
filters.filter('Table1', 'pkey1 == 1')

print(filters.tables_manager.tables['Table1'])
print(filters.tables_manager.tables['Table2'])

```
