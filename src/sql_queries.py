# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES
songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplay(
                             songplay_id SERIAL PRIMARY KEY,
                             start_time BIGINT NOT NULL references time_table(start_time),
                             user_id INT NOT NULL references user_table(user_id),
                             userlevel VARCHAR,
                             song_id VARCHAR references song(song_id),
                             artist_id VARCHAR references artist(artist_id),
                             session_id INT,
                             location VARCHAR,
                             user_agent VARCHAR
                         );
""")

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS user_table(
                         user_id INT PRIMARY KEY,
                         firstname VARCHAR,
                         lastname VARCHAR,
                         gender VARCHAR(1),
                         userlevel VARCHAR
                     );
""")


artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artist(
                           artist_id VARCHAR PRIMARY KEY,
                           name VARCHAR NOT NULL,
                           location VARCHAR,
                           longitude FLOAT,
                           latitude FLOAT
                       );
""")

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS song(
                         song_id VARCHAR PRIMARY KEY,
                         title VARCHAR NOT NULL,
                         artist_id VARCHAR NOT NULL references artist(artist_id),
                         year INT,
                         duration FLOAT NOT NULL
                     );
""")


# Note: start time is the UTC timestamp in milliseconds, like 1541106106796. For that, a regular int won't suffice. We need a "BIGINT"
time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time_table(
                         start_time BIGINT PRIMARY KEY,
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
                         INSERT INTO songplay (start_time, user_id, userlevel, song_id, artist_id, session_id, location, user_agent)
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                         ON CONFLICT DO NOTHING;
                         """)

"""
Note: Users can change their level (paid or free). I will that they are also allowed to update their other data as well
"""
user_table_insert = ("""
                     INSERT INTO user_table(user_id, firstname, lastname, gender, userlevel)
                     VALUES(%s, %s, %s, %s, %s)
                     ON CONFLICT (user_id) DO UPDATE SET
                     firstname = COALESCE(EXCLUDED.firstname, user_table.firstname),
                     lastname = COALESCE(EXCLUDED.lastname, user_table.lastname),
                     gender = COALESCE(EXCLUDED.gender, user_table.gender),
                     userlevel = COALESCE(EXCLUDED.userlevel, user_table.userlevel);

""")

song_table_insert = ("""
                     INSERT INTO song (song_id, title, artist_id, year, duration)
                     VALUES (%s, %s, %s, %s, %s)
                     ON CONFLICT DO NOTHING;
                     
""")

"""
Since artists exist across multiple entries in the JSON files, it can be the case that in one file there's missing data, while in another 
the data is there. As such, I opted to use the COALESCE function to keep the first non-null value.
In this query, I use COALESCE, which returns the first non-null in the arguments. If there's a conflicting 
item to be inserted, and it turns out to update pre-existing null value, then take it. If not, stick with the old value
"""
artist_table_insert = ("""
                       INSERT INTO artist(artist_id, name, location, longitude, latitude)
                       VALUES(%s, %s, %s, %s, %s)
                       ON CONFLICT(artist_id)
                       DO UPDATE SET
                       longitude = COALESCE(artist.longitude, EXCLUDED.longitude),
                       latitude = COALESCE(artist.latitude, EXCLUDED.latitude),
                       name = COALESCE(artist.name, EXCLUDED.name),
                       location = COALESCE(artist.location, EXCLUDED.location);
""")


time_table_insert = ("""
                     INSERT INTO time_table(start_time, hour, day, weekday, week, month, year)
                     VALUES(%s, %s, %s, %s, %s, %s, %s)
                     ON CONFLICT DO NOTHING;
""")

# FIND SONGS
song_select = ("""
               SELECT song.song_id, artist.artist_id
               FROM song
               JOIN artist ON song.artist_id = artist.artist_id
               WHERE %s = song.title AND %s = artist.name AND %s = song.duration;
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