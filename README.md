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
from crossfilter_dataframe import (
    CrossFilters, 
    TablesManager, 
    loader_factory,
    table_factory
)

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
loader = loader_factory(kind='dict')(data=relations)
    
# setup
tables = TablesManager(spec_loader=loader)
tables.add_table('Table1', table_factory('pandas', df1)) 
tables.add_table('Table2', table_factory('pandas', df2)) 

# prepare for filter
filters = CrossFilters(tables_manager=tables)
filters.filter('Table1', 'pkey1 == 1')

# collect data
filtered_df1 = filters.collect('Table1')
filtered_df2 = filters.collect('Table2')

```
