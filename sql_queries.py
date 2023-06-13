import configparser


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

# CREATE TABLES to create all tables

# CREATE TABLE "sporting_event_ticket" (
#     "id" double precision DEFAULT nextval('sporting_event_ticket_seq') NOT NULL,
#     "sporting_event_id" double precision NOT NULL,
#     "sport_location_id" double precision NOT NULL,
#     "seat_level" numeric(1,0) NOT NULL,
#     "seat_section" character varying(15) NOT NULL,
#     "seat_row" character varying(10) NOT NULL,
#     "seat" character varying(10) NOT NULL,
#     "ticketholder_id" double precision,
#     "ticket_price" numeric(8,2) NOT NULL
# );

# {"num_songs": 1, 
# "artist_id": "ARJIE2Y1187B994AB7", 
# "artist_latitude":null, 
# "artist_longitude":null, 
# "artist_location": "", 
# "artist_name": "Line Renaud", 
# "song_id": "SOUPIRU12A6D4FA1E1", 
# "title": "Der Kleine Dompfaff", 
# "duration": 152.92036, 
# "year": 0}


staging_songs_table_create = ("""
    CREATE TABLE "staging_song_table" (
    "artist_id" character varying(15) NOT NULL,
    "artist_latitude" character varying(15) NOT NULL,
    "artist_longitude" character varying(15) NOT NULL,
    "artist_name" character varying(15) NOT NULL,
    "song_id" character varying(15) NOT NULL,
    "title" character varying(15) NOT NULL,
    "duration" double precision NOT NULL,
    "year" integer
);
""")

staging_events_table_create= ("""
    CREATE TABLE "staging_event_table" (
    "artist" character varying(15) NOT NULL,
    "auth" character varying(15) NOT NULL,
    "firstName" character varying(15) NOT NULL,
    "gender" character varying(15) NOT NULL,
    "itemInSession" integer NOT NULL,
    "lastName" character varying(15) NOT NULL,
    "length" double precision NOT NULL,
    "level" character varying(15),
    "location" character varying(15),
    "method" character varying(15),
    "page" character varying(15),
    "registration" double precision,
    "sessionId" integer,
    "song" character varying(15),
    "status" integer,
    "ts" integer,
    "userAgent" character varying(15),
    "userId" integer NOT NULL
);
""")



songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# COPY Queries from S3 bucket into STAGING TABLES
# [IAM_ROLE]
# ARN='project2User'
# [S3]
# LOG_DATA='s3://udacity-dend/log_data'


# COPY staging_events
#     FROM {}
#     iam_role '{}'
#     region 'us-west-2'
#     TIMEFORMAT AS 'epochmillisecs'
#     JSON {};

# load partitioned data into the cluster
# from 2-cloudDataWarehouses/4- implementing A DataWarehouseOnAWS/L3 Exercise 3 - Parallel ETL - Solution.ipynb
# qry = """
#     copy sporting_event_ticket from 's3://udacity-labs/tickets/split/part'
#     credentials 'aws_iam_role={}'
#     gzip delimiter ';' compupdate off region 'us-west-2';
# """.format(DWH_ROLE_ARN)

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
""").format()

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

# QUERY LISTS

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                       song_table_drop, artist_table_drop, time_table_drop]

#Query that creates all the tables needed staging AND Fact and Dimension tables
create_table_queries = [staging_events_table_create, staging_songs_table_create, 
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

# Query to copy data from the s3 buckets into the 2 staging tables
copy_table_queries = [staging_events_copy, staging_songs_copy]

# Query that inserts in the Fact and Dimension tables data that was copied within the staging tables
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, 
                        time_table_insert]
