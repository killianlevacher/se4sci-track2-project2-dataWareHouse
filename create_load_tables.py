import configparser
import psycopg2
from sql_queries import drop_table_queries, create_table_queries, copy_table_queries

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')



def drop_tables(cur, conn):
    '''
    Drops any tables previously created to get a fresh start
    '''
    print("Dropping Tables")
    for query in drop_table_queries:
        print("QUERY: {}".format(query))
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates all the tables required for the project
    '''
    print("Creating Tables")
    for query in create_table_queries:
        if query.strip() != "":
            print("QUERY: {}".format(query))
            cur.execute(query)
            conn.commit()



def load_staging_tables(cur, conn):
    '''
    Loads staging tables with dataset available in S3
    '''
    for query in copy_table_queries:
        if query.strip() != "":
            print("QUERY: {}".format(query))
            cur.execute(query)
            conn.commit()


def main():
    '''
    Main function that 
    a) drops any table created previously, 
    b) creates all the tables necessary for the project and 
    c) loads the datasets inside the staging tables
    Once this script is run, the tables are ETL ready
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_staging_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()