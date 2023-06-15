import configparser
import psycopg2

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
    "duration" varchar,
    "year" varchar
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
    "ts" TIMESTAMP,
    "userAgent" varchar,
    "userId" integer 
);
""")
                              




################################# FACT TABLES

user_table_create = ("""
    CREATE TABLE "user_table" (
    "user_id" integer NOT NULL,
    "first_name" character varying(15),
    "last_name" character varying(15) ,
    "gender" character varying(15),
    "level" character varying(15),
    PRIMARY KEY (user_id)
);
""")

artist_table_create = ("""
    CREATE TABLE "artist_table" (
    "artist_id" varchar NOT NULL,
    "artist_name" varchar,
    "location" varchar,
    "lattitude" varchar,
    "longitude" varchar,
    PRIMARY KEY (artist_id)
);
""")                

song_table_create = ("""
    CREATE TABLE "song_table" (
    "song_id" varchar NOT NULL,
    "title" varchar NOT NULL,
    "artist_id" varchar NOT NULL,
    "year" varchar,
    "duration" varchar,
    PRIMARY KEY (song_id),
    FOREIGN KEY (artist_id) REFERENCES artist_table(artist_id)
);
""")
                     
time_table_create = ("""
    CREATE TABLE "time_table" (
    "start_time" TIMESTAMP NOT NULL,
    "hour" integer,
    "day" integer,
    "week" integer,
    "month" integer,
    "year" varchar,
    "weekday" double precision,
    PRIMARY KEY (start_time)
);
""")

songplay_table_create = ("""
    CREATE TABLE "songplay_table" (
    "songplay_id" BIGINT NOT NULL IDENTITY(0,1),
    "start_time" TIMESTAMP,
    "user_id" integer ,
    "level" varchar ,
    "song_id" varchar ,
    "artist_id" varchar,
    "session_id" varchar,
    "location" varchar,
    "user_agent" varchar,
    CONSTRAINT user_session_iid PRIMARY KEY (songplay_id),
    FOREIGN KEY (user_id) REFERENCES user_table(user_id),
    FOREIGN KEY (song_id) REFERENCES song_table(song_id),
    FOREIGN KEY (artist_id) REFERENCES artist_table(artist_id)
);
""")
                         
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
                     
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                       song_table_drop, artist_table_drop, time_table_drop]


#Query that creates all the tables needed staging AND Fact and Dimension tables
create_table_queries = [staging_events_table_create, staging_songs_table_create, 
                        user_table_create,  artist_table_create, song_table_create, time_table_create, songplay_table_create, ]


# Query to copy data from the s3 buckets into the 2 staging tables
copy_table_queries = [staging_events_copy, staging_songs_copy]

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



def load_staging_tables(cur, conn):
    for query in copy_table_queries:
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
    load_staging_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()