from crossfilter_dataframe.graphs.dag import DAGExecutor, DAGProcessor
from crossfilter_dataframe.logger import logging
from crossfilter_dataframe.tables.manager import TablesManager


class CrossFilters:
    def __init__(self, tables_manager: TablesManager, relational_graph):
        # stateful processor - graph is the same, just direct is different
        self.processor = DAGProcessor(graph=relational_graph)
        self.tables_manager = tables_manager  # container fns to interact with tables
    
    def _executor_build_filters_orders(self, dag):
        executor = DAGExecutor(dag=dag)
        executor.add_callback(self.tables_manager.build_downstream)
        executor.walk()
        
        return executor.callbacks_results
    
    def _apply_filter_on_tables(self, _key: str, *args, **kwargs):
        start_table = self.tables_manager.get_table(_key)
        start_table.filter(*args, **kwargs)

        # once the filter chain is completed, reset all the downstreams
        self.tables_manager.reset_all_dwnstreams()
    
    def filter(self, start_table_key: str, *filter_args, **filter_kwargs):
        # when filter starts, use a relational graph to convert to a DAG
        dag = self.processor.process(start_root=start_table_key)
        results = self._executor_build_filters_orders(dag)
        self._apply_filter_on_tables(start_table_key, *filter_args, **filter_kwargs)
                
        
if __name__ == '__main__':
    pass