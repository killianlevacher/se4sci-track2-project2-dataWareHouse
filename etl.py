import configparser
import psycopg2
from sql_queries import final_count_queries, insert_table_queries

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


def insert_tables(cur, conn):
    '''
    Runs all the SQL queries required to insert the data from the staging tables into the fact and dimension tables
    '''
    for query in insert_table_queries:
        if query.strip() != "":
            print("QUERY: {}".format(query))
            cur.execute(query)
            conn.commit()

def run_final_count_queries(cur, conn):
    '''
    Runs a set of queries to count the number of entries in each table
    '''
    print("Running count queries")
    for query in final_count_queries:
        if query.strip() != "":
            print("QUERY: {}".format(query))
            cur.execute(query)
            conn.commit()
            row = cur.fetchone()
            while row:
                print("{} entries".format(row[0]))
                print()
                row = cur.fetchone()

def main():
    '''
    Extracts data located in the staging tables and inserts them properly in the fact and dimenension tables
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    
    insert_tables(cur, conn)
    run_final_count_queries(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()