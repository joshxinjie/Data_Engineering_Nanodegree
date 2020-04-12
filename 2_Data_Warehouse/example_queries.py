import configparser
import psycopg2
from sql_queries import analytics_table_example_queries

def execute_queries(cur, conn):
    """
    Execute example queries on the analytics tables to test the
    database and the ETL pipeline
    
    @type config_file_path: str
    @rtype config_dict: dict
    """
    for query in analytics_table_example_queries:
        print("Running Query...")
        print(query)
        cur.execute(query)
        results = cur.fetchall()

        for row in results:
            print("   ", row)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    execute_queries(cur, conn)
    
    conn.close()