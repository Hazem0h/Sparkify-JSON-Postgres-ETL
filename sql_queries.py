# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

# TODO is the songplay a primary Key? What are the datatypes of this table?
# TODO is the level column necessary? I don't think it is.
songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplay(
                             songplay_id INT,
                             start_time FLOAT,
                             user_id INT references user(user_id),
                             level INT,
                             song_id INT references song(song_id),
                             artist_id INT references artist(artist_id),
                             session_id INT,
                             location VARCHAR,
                             user_agent VARCHAR
                         );
""")

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS user(
                         user_id INT PRIMARY KEY,
                         first_name VARCHAR,
                         last_name VARCHAR,
                         gender VARCHAR(1),
                         level INT
                     );
""")


artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artist(
                           artist_id INT PRIMARY KEY,
                           name VARCHAR,
                           location VARCHAR,
                           latitude FLOAT,
                           longitude FLOAT
                       );
""")

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS song(
                         song_id INT PRIMARY KEY,
                         title VARCHAR,
                         artist_id INT references artist(artist_id),
                         year INT,
                         duration FLOAT
                     );
""")

# TODO I'm not sure about this table's datatypes, or even its existence
# TODO Maybe we need a lot of constraints here
time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time_table(
                         start_time FLOAT,
                         hour INT,
                         day INT,
                         week INT,
                         year INT,
                         weekday INT
                     );
""")

# INSERT RECORDS

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

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]