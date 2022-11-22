# DROP TABLES

songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

songplay_table_create = ("""
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

time_table_create = ("""
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