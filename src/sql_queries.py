# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

# TODO is the songplay a primary Key? What are the datatypes of this table?
# TODO is the level column necessary? I don't think it is.
songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplay(
                             songplay_id INT,
                             start_time FLOAT,
                             user_id VARCHAR references user_table(user_id),
                             level INT,
                             song_id VARCHAR,
                             artist_id VARCHAR,
                             session_id INT,
                             location VARCHAR,
                             user_agent VARCHAR
                         );
""")

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS user_table(
                         user_id VARCHAR PRIMARY KEY,
                         firstname VARCHAR,
                         lastname VARCHAR,
                         gender VARCHAR(1),
                         userlevel VARCHAR
                     );
""")


artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artist(
                           artist_id VARCHAR PRIMARY KEY,
                           name VARCHAR,
                           location VARCHAR,
                           latitude FLOAT,
                           longitude FLOAT
                       );
""")

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS song(
                         song_id VARCHAR PRIMARY KEY,
                         title VARCHAR,
                         artist_id VARCHAR,
                         year INT,
                         duration FLOAT
                     );
""")


# Note: start time is the UTC timestamp in milliseconds, like 1541106106796. For that, a regular int won't suffice. We need a "BIGINT"
time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time_table(
                         start_time BIGINT,
                         hour INT,
                         day INT,
                         weekday INT,
                         week INT,
                         month INT,
                         year INT
                     );
""")

# INSERT RECORDS

songplay_table_insert = ("""
                         INSERT INTO songplay (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location VARCHAR, user_agent)
                         VALUES (%s, %s, %s, %d, %s, %s, %s, %s, %s);
                         """)

user_table_insert = ("""
                     INSERT INTO user_table(user_id, firstname, lastname, gender, userlevel)
                     VALUES(%s, %s, %s, %s, %s)
                     ON CONFLICT (user_id) DO UPDATE SET
                     firstname = EXCLUDED.firstname,
                     lastname = EXCLUDED.lastname,
                     gender = EXCLUDED.gender,
                     userlevel = EXCLUDED.userlevel;

""")

song_table_insert = ("""
                     INSERT INTO song (song_id, title, artist_id, year, duration)
                     VALUES (%s, %s, %s, %s, %s);
""")

artist_table_insert = ("""
                       INSERT INTO artist(artist_id, name, location, latitude, longitude)
                       VALUES(%s, %s, %s, %s, %s);
""")


time_table_insert = ("""
                     INSERT INTO time_table(start_time, hour, day, weekday, week, month, year)
                     VALUES(%s, %s, %s, %s, %s, %s, %s);
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS
'''
# Note
Due to the foreign keys constraints, the tables need to be dropped in a certain order (the parent tables are dropped first, then the dependent tables)
Otherwise, this will cause errors

Similarly, in table creation, we can't make a foreign key point to a table that doesn't yet exist.

Also, also, similarly in data insertion, we will have to fill the dimension tables first, before the fact table, since the fact table has foreign key
constraints
'''
create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, artist_table_drop, song_table_drop, time_table_drop, songplay_table_drop]