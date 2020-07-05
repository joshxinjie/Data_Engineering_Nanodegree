from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Operator to run checks on SQL tables in AWS Redshift. At the basic
    level, it will check if the table is not empty. An optional check
    will be to check if there are NULL values in any specified columns.
    
    :param redshift_conn_id             The Redshift connection id
    :type redshift_conn_id              string
    :param table                        The SQL table name
    :type table                         string
    :param check_null                   The parameter for specifying
                                        whether to check for NULL
                                        values in specified columns
    :type check_null                    boolean
    :param columns                      The columns in the SQL table
                                        that will be checked for NULL
                                        values.
    :type columns                       list of strings
    """

    ui_color = '#89DA59'
    no_null_sql = "SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
    not_empty_sql = "SELECT COUNT(*) FROM {table}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 check_null=False,
                 columns=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.check_null=check_null
        self.columns = columns

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # check for proper insertion of data
        self.log.info(f'Checking for entries in table `{self.table}`...')
        formatted_not_empty_sql = DataQualityOperator.not_empty_sql.format(
            table = self.table
        )
        records = redshift.get_records(formatted_not_empty_sql)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        else:
            self.log.info(f'Data quality check passed: {len(records)} rows inserted into \
                    table `{self.table}`')
        
        # check for null values
        if self.check_null:
            for col in self.columns:
                self.log.info(f'Checking for NULL values in column `{col}` \
                    of table `{self.table}`...')
                formatted_no_null_sql = DataQualityOperator.no_null_sql.format(
                    col = col,
                    table = self.table
                )
                records = redshift.get_records(formatted_not_empty_sql)
                num_nulls = records[0][0]
                if num_nulls > 0:
                    raise ValueError(f"Data quality check failed: {num_nulls} NULL values \
                        present in column `{col}` of table `{self.table}`")
                else:
                    self.log.info(f'Data quality check passed: No NULL values in column `{col}` \
                        of table `{self.table}`')
        
        self.log.info(f'Data quality checks for {self.table} passed')