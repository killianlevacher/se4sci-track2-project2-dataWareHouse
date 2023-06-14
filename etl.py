import configparser
import psycopg2
# from sql_queries import copy_table_queries, insert_table_queries

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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


# staging_events_table_create= ("""
#     CREATE TABLE "staging_event_table" (
#     "artist" varchar,
#     "auth" varchar,
#     "firstName" varchar,
#     "gender" varchar,
#     "itemInSession" integer ,
#     "lastName" varchar,
#     "length" double precision ,
#     "level" varchar,
#     "location" varchar,
#     "method" varchar,
#     "page" varchar,
#     "registration" double precision,
#     "sessionId" integer,
#     "song" varchar,
#     "status" integer,
#     "ts" BIGINT,
#     "userAgent" varchar,
#     "userId" integer 
# );
# """)

# user_table_create = ("""
#     CREATE TABLE "user_table" (
#     "user_id" integer,
#     "first_name" character varying(15) NOT NULL,
#     "last_name" character varying(15) NOT NULL,
#     "gender" character varying(15) NOT NULL,
#     "level" character varying(15) NOT NULL
# );
# """)

user_table_insert = ("""
INSERT INTO user_table (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(e.userId) AS userId,
       e.firstName AS first_name,
       e.lastName AS last_name,
       e.gender AS gender,
       e.level AS level
FROM staging_event_table e
Where e.userId is not null
""")
                     
# INSERT QUERIES to create FINAL TABLES

# INSERT INTO dimDate (date_key, date, year, quarter, month, day, week, is_weekend)
# SELECT DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer) AS date_key,
#        date(payment_date)                                           AS date,
#        EXTRACT(year FROM payment_date)                              AS year,
#        EXTRACT(quarter FROM payment_date)                           AS quarter,
#        EXTRACT(month FROM payment_date)                             AS month,
#        EXTRACT(day FROM payment_date)                               AS day,
#        EXTRACT(week FROM payment_date)                              AS week,
#        CASE WHEN EXTRACT(ISODOW FROM payment_date) IN (6, 7) THEN true ELSE false END AS is_weekend
# FROM payment;


### Useful SQL Queries
# Dealing with duplicates
# SELECT user_id, COUNT(*) as count FROM user_table GROUP BY user_id ORDER BY count DESC;
# SELECT user_id, first_name, last_name FROM user_table GROUP BY user_id, first_name, last_name ORDER BY user_id DESC;




songplay_table_insert = ("""
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