import configparser
import psycopg2
# from sql_queries import create_table_queries

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

########################### STAGING TABLES

staging_songs_table_create = ("""
    CREATE TABLE "staging_song_table" (
    "artist_id" varchar,
    "artist_latitude" varchar,
    "artist_longitude" varchar,
    "artist_name" varchar,
    "song_id" varchar,
    "title" varchar,
    "duration" double precision,
    "year" integer
);
""")

                              
staging_events_table_create= ("""
    CREATE TABLE "staging_event_table" (
    "artist" varchar,
    "auth" varchar,
    "firstName" varchar,
    "gender" varchar,
    "itemInSession" integer ,
    "lastName" varchar,
    "length" double precision ,
    "level" varchar,
    "location" varchar,
    "method" varchar,
    "page" varchar,
    "registration" double precision,
    "sessionId" integer,
    "song" varchar,
    "status" integer,
    "ts" BIGINT,
    "userAgent" varchar,
    "userId" integer 
);
""")
                              




################################# FACT TABLES

# 1. **songplays** - records in event data associated with song plays i.e. records with page `NextSong`
#     - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

# ### **Dimension Tables**

# 1. **users** - users in the app
#     - *user_id, first_name, last_name, gender, level*
# 2. **songs** - songs in music database
#     - *song_id, title, artist_id, year, duration*
# 3. **artists** - artists in music database
#     - *artist_id, name, location, lattitude, longitude*
# 4. **time** - timestamps of records in **songplays** broken down into specific units
#     - *start_time, hour, day, week, month, year, weekday*


##### TODO Make sure to create primary keys
# TODO find out what start time is from ts in 'epochmillisecs'
# TODO songplay_id should be auto generated 
songplay_table_create = ("""
    CREATE TABLE "songplay_table" (
    "-----songplay_id" BIGINT NOT NULL,
    "start_time" BIGINT,
    "user_id" integer NOT NULL,
    "level" character varying(15) NOT NULL,
    "song_id" character varying(15) NOT NULL,
    "artist_id" character varying(15) NOT NULL,
    "session_id" integer,
    "location" character varying(50),
    "user_agent" character varying(15)
);
""")
                         


user_table_create = ("""
    CREATE TABLE "user_table" (
    "user_id" integer,
    "first_name" character varying(15) NOT NULL,
    "last_name" character varying(15) NOT NULL,
    "gender" character varying(15) NOT NULL,
    "level" character varying(15) NOT NULL
);
""")
                     

song_table_create = ("""
    CREATE TABLE "song_table" (
    "song_id" character varying(15) NOT NULL,
    "title" character varying(15) NOT NULL,
    "artist_id" character varying(15) NOT NULL,
    "year" integer,
    "duration" double precision
);
""")
                     


artist_table_create = ("""
    CREATE TABLE "artist_table" (
    "artist_id" character varying(15) NOT NULL,
    "artist_name" character varying(15) NOT NULL,
    "location" character varying(50) NOT NULL,
    "lattitude" character varying(15) NOT NULL,
    "longitude" character varying(15) 
);
""")
                       

# TODO find out what start time is from ts in 'epochmillisecs'
time_table_create = ("""
    CREATE TABLE "time_table" (
    "start_time" BIGINT,
    "hour" integer,
    "day" integer,
    "week" integer,
    "month" integer,
    "year" integer,
    "weekday" double precision
);
""")
                     
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                       song_table_drop, artist_table_drop, time_table_drop]


#Query that creates all the tables needed staging AND Fact and Dimension tables
create_table_queries = [staging_events_table_create, staging_songs_table_create, 
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]



def drop_tables(cur, conn):
    print("Dropping Tables")
    for query in drop_table_queries:
        print("QUERY: {}".format(query))
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    print("Creating Tables")
    for query in create_table_queries:
        if query.strip() != "":
            print("QUERY: {}".format(query))
            cur.execute(query)
            conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()