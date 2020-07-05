from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Operator to load SQL fact tables in AWS Redshift
    
    :param redshift_conn_id             The Redshift connection id
    :type redshift_conn_id              string
    :param table                        The SQL table name
    :type table                         string
    :param insert_sql_query             The SQL query for loading data 
                                        into the fact table
    :type insert_sql_query              string
    :param truncate                     The parameter for specifying 
                                        append-only and delete-load 
                                        functionality. If true, will 
                                        perform fact loads with
                                        delete-load functionality.
    :type truncate                      boolean
    """

    ui_color = '#F98866'
    table_insert_sql = """
        INSERT INTO {table}
        {insert_sql_query};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_sql_query="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql_query = insert_sql_query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:        
            self.log.info(f"Clearing data from {self.table} table...")
            redshift.run("DELETE FROM {}".format(self.table))
            self.log.info(f"Clearing data from {self.table} table is completed")
        
        self.log.info(f'Loading data into {self.table} fact table...')
        formatted_table_insert_sql = LoadFactOperator.table_insert_sql.format(
            table=self.table,
            insert_sql_query=self.insert_sql_query
        )
        redshift.run(formatted_table_insert_sql)
        self.log.info(f'Loading of data into {self.table} fact table is completed')