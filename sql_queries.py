import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS 'sporting_event_ticket';"
staging_songs_table_drop = "DROP TABLE IF EXISTS 'staging_songs_table_drop';"
songplay_table_drop = "DROP TABLE IF EXISTS 'songplay_table_drop';"
user_table_drop = "DROP TABLE IF EXISTS 'user_table_drop';"
song_table_drop = "DROP TABLE IF EXISTS 'song_table_drop';"
artist_table_drop = "DROP TABLE IF EXISTS 'artist_table_drop';"
time_table_drop = "DROP TABLE IF EXISTS 'time_table_drop';"

# CREATE TABLES

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

staging_events_table_create= ("""


""")

staging_songs_table_create = ("""
    CREATE TABLE "staging_song_table" (
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

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

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

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, 
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                       song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, 
                        time_table_insert]
