import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


staging_events_copy = ("""
    COPY staging_event_table
    FROM {}
    iam_role {}
    region 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    JSON {};
""").format(config['S3']['LOG_DATA'], 
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH'])



staging_songs_copy = ("""
copy staging_song_table from {} 
iam_role {}
format as JSON 'auto'
region 'us-west-2'
TIMEFORMAT AS 'epochmillisecs';
""").format(config['S3']['SONG_DATA'], 
            config['IAM_ROLE']['ARN'])


# INSERT QUERIES to create FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# Query to copy data from the s3 buckets into the 2 staging tables
copy_table_queries = [staging_events_copy, staging_songs_copy]


# Query that inserts in the Fact and Dimension tables data that was copied within the staging tables
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, 
                        time_table_insert]

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        if query.strip() != "":
            print("QUERY: {}".format(query))
            cur.execute(query)
            conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        if query.strip() != "":
            print("QUERY: {}".format(query))
            cur.execute(query)
            conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()