from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql_query = "",
                 append_mode=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_mode=append_mode

    def execute(self, context):
        
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(self.redshift_conn_id)
        redshift.run(str(self.sql_query))
        
        