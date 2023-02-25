from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_query='',
                 table='',
                 append_mode=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.table=table
        self.append_mode=append_mode

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        #insert_sql = f"INSERT INTO {self.table}\n" + self.query
        #redshift.run(insert_sql)
        redshift = PostgresHook(self.redshift_conn_id)
        redshift.run(str(self.sql_query))
        
        
