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

########################### STAGING TABLES

staging_songs_table_create = ("""
    CREATE TABLE "staging_song_table" (
    "artist_id" varchar,
    "artist_latitude" DECIMAL,
    "artist_longitude" DECIMAL,
    "artist_location" varchar, 
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
    "latitude" DECIMAL,
    "longitude" DECIMAL,
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
                         

################################# STAGING TABLES
                         
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
                       
    # cast(e.artist_longitude as decimal)
    #        e.artist_latitude AS lattitude,
    #    e.artist_longitude AS longitude,
# artist_table_insert = ("""
# INSERT INTO artist_table (artist_id, artist_name, location, lattitude, longitude)
# SELECT DISTINCT(e.artist_id) AS artist_id,
#        e.artist_name AS artist_name,
#        cast(e.artist_latitude as decimal) as latitude,
#        cast(e.artist_longitude as decimal) as longitude,
#        a.location as location
# FROM staging_song_table e
# JOIN staging_event_table a  ON (e.artist_name = a.artist)
# Where e.artist_id is not null
# """)

# staging_songs_table_create = ("""
#     CREATE TABLE "staging_song_table" (
#     "artist_id" varchar,
#     "artist_latitude" DECIMAL,
#     "artist_longitude" DECIMAL,
#     "artist_location" varchar, 
#     "artist_name" varchar,
#     "song_id" varchar,
#     "title" varchar,
#     "duration" varchar,
#     "year" varchar
# );
# """)        

artist_table_insert = ("""
INSERT INTO artist_table (artist_id, artist_name, location, latitude, longitude)
SELECT DISTINCT(e.artist_id) AS artist_id,
       e.artist_name AS artist_name,
       e.artist_location as location,
       cast(e.artist_latitude as DECIMAL) as latitude,
       cast(e.artist_longitude as DECIMAL) as longitude
FROM staging_song_table e
Where e.artist_id is not null
""")


# "artist_location" varchar, 
# "location" varchar,        

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