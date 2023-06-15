import configparser
import psycopg2

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


time_table_insert = ("""
INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
SELECT
    e.ts as start_time,
    EXTRACT(hour FROM CAST(e.ts AS DATE)) AS hour,
    EXTRACT(day FROM CAST(e.ts AS DATE)) AS day,
    EXTRACT(week FROM CAST(e.ts AS DATE)) AS week,
    EXTRACT(month FROM CAST(e.ts AS DATE)) AS month,
    EXTRACT(year FROM CAST(e.ts AS DATE)) AS year,
    DATE_PART(dayofweek, e.ts) AS weekday
FROM staging_event_table e
""")


songplay_table_insert = ("""
INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    e.ts AS start_time,
    e.userId AS user_id,
    e.level AS level, 
    a.song_id AS song_id, 
    a.artist_id AS artist_id, 
    e.sessionId AS session_id, 
    e.location AS location, 
    e.userAgent AS user_agent
FROM staging_event_table e
JOIN staging_song_table a  ON (e.song = a.title)
Where (e.sessionId is not null)
""")


song_table_insert = ("""
INSERT INTO song_table (song_id, title, artist_id, year, duration)
SELECT DISTINCT(e.song_id) AS song_id,
       e.title AS title,
       e.artist_id AS artist_id,
       e.year AS year,
       e.duration AS duration

FROM staging_song_table e
Where e.song_id is not null

""")
                       
    
artist_table_insert = ("""
INSERT INTO artist_table (artist_id, artist_name, location, lattitude, longitude)
SELECT DISTINCT(e.artist_id) AS artist_id,
       e.artist_name AS artist_name,
       e.artist_latitude AS lattitude,
       e.artist_longitude AS longitude,
       a.location as location
FROM staging_song_table e
JOIN staging_event_table a  ON (e.artist_name = a.artist)
Where e.artist_id is not null
""")

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
                     

# Query that inserts in the Fact and Dimension tables data that was copied within the staging tables
insert_table_queries = [
                        user_table_insert, 
                        songplay_table_insert, 
                        song_table_insert, 
                        artist_table_insert,
                        time_table_insert
                        ]



count_staging_event_table_query = ("""
SELECT COUNT(*) FROM staging_event_table
 """)

count_staging_song_table_query = ("""
SELECT COUNT(*) FROM staging_song_table
 """)

count_songplay_query = ("""
SELECT COUNT(*) FROM songplay_table
 """)

count_user_table_query = ("""
SELECT COUNT(*) FROM user_table
 """)

count_song_table_query = ("""
SELECT COUNT(*) FROM song_table
 """)

count_artist_table_query = ("""
SELECT COUNT(*) FROM artist_table
 """)

count_time_table_query = ("""
SELECT COUNT(*) FROM time_table
 """)

final_count_queries = [
    count_staging_event_table_query,
    count_staging_song_table_query,
    count_songplay_query, 
    count_user_table_query, 
    count_song_table_query, 
    count_artist_table_query, 
    count_time_table_query
]

def insert_tables(cur, conn):
    for query in insert_table_queries:
        if query.strip() != "":
            print("QUERY: {}".format(query))
            cur.execute(query)
            conn.commit()

def run_final_count_queries(cur, conn):
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
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    
    insert_tables(cur, conn)
    run_final_count_queries(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()