import os
import glob
import json
from typing import List
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extensions import register_adapter
import sql_queries


errors = []

def connect_to_db(config_path = "../config.json"):
    """Tries to connect to a Postgres database

    Keyword Arguments:
        config_path -- the path to the file containing database configuration (default: {"../config.json"})

    Returns:
        the database cursor, and the connection object
    """
    with open(config_path) as f:
        config = json.load(f)
        conn = psycopg2.connect(host = config["host"],
                                dbname = config["dbname"],
                                user = config["username"],
                                password = config["password"])

        conn.set_session(autocommit=True)
        cur = conn.cursor()
        return conn, cur

def setup_adapters():
    """Registers psycopg2 adapters to help psycopg2 convert unfamiliar types into their correct 
    string representation
    """
    def adapt_np_int(np_int:np.int64):
        _INT = psycopg2.extensions.Int
        return _INT(np_int)
    register_adapter(np.int64, adapt_np_int)

def fill_artist_table(cur, path = "../data/cleaned/artists.csv"):
    """Fills the artist table of the Postgres database with the data in the csv file specified by the path

    Arguments:
        cur -- the database cursor object

    Keyword Arguments:
        path -- path to the csv file containing the artist data (default: {"../data/cleaned/artists.csv"})
    """
    df_artist = pd.read_csv(path)
    df_artist.fillna(psycopg2.extensions.AsIs("NULL"), inplace = True)
    for index, row in df_artist.iterrows():
        try:
            cur.execute(sql_queries.artist_table_insert, list(row.values))
        except psycopg2.errors.NotNullViolation as e:
            errors.append(e)
    
def fill_song_table(cur, path = "../data/cleaned/songs.csv"):
    """Fills the song table of the Postgres database with the data in the csv file specified by the path

    Arguments:
        cur -- the database cursor object

    Keyword Arguments:
        path -- path to the csv file containing the song data (default: {"../data/cleaned/songs.csv"})
    """
    df_songs = pd.read_csv(path)
    df_songs.fillna(psycopg2.extensions.AsIs("NULL"), inplace = True)
    for index, row in df_songs.iterrows():
        try:
            cur.execute(sql_queries.song_table_insert, list(row.values))
        except psycopg2.errors.NotNullViolation as e:
            errors.append(e)
        except psycopg2.errors.ForeignKeyViolation as e:
            errors.append(e)
            
            

def fill_time_table(cur, path = "../data/cleaned/time.csv"):
    """Fills the time table of the Postgres database with the data in the csv file specified by the path

    Arguments:
        cur -- the database cursor object

    Keyword Arguments:
        path -- path to the csv file containing the time data (default: {"../data/cleaned/time.csv"})
    """
    df_times = pd.read_csv(path)
    df_times.fillna(psycopg2.extensions.AsIs("NULL"), inplace = True)
    for index, row in df_times.iterrows():
        cur.execute(sql_queries.time_table_insert, list(row.values))
    
def fill_users_table(cur, path = "../data/cleaned/users.csv"):
    """Fills the user table of the Postgres database with the data in the csv file specified by the path

    Arguments:
        cur -- the database cursor object

    Keyword Arguments:
        path -- path to the csv file containing the user data (default: {"../data/cleaned/users.csv"})
    """
    df_users = pd.read_csv(path)
    df_users.fillna(psycopg2.extensions.AsIs("NULL"), inplace = True)
    for index, row in df_users.iterrows():
        cur.execute(sql_queries.user_table_insert, list(row.values))

def fill_songplays_table(cur, path = "../data/cleaned/songplays.csv"):
    """Fills the songplays table of the Postgres database with the data in the csv file specified by the path,
    as well as using data from the other dimension tables

    Arguments:
        cur -- the database cursor object

    Keyword Arguments:
        path -- path to the csv file containing the songplays data (default: {"../data/cleaned/artists.csv"})
    """
    df_songplays = pd.read_csv(path)
    for index, row in df_songplays.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(sql_queries.song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row["ts"], row["userId"], row["level"], songid, artistid, row["sessionId"], row["location"], row["userAgent"])
        try:
            cur.execute(sql_queries.songplay_table_insert, songplay_data)
        except psycopg2.errors.UniqueViolation as e:
            errors.append(e)
        except psycopg2.errors.NotNullViolation as e:
            errors.append(e)

def load_pg():
    """The main function of loading data to Postgres
    """
    conn, cur = connect_to_db()
    print("connected to database")
    setup_adapters()
    print("adapters setup")
    fill_artist_table(cur)
    print("artist table filled")
    fill_song_table(cur)
    print("song table filled")
    fill_time_table(cur)
    print("time table filled")
    fill_users_table(cur)
    print("user table filled")
    fill_songplays_table(cur)
    print("songplays table filled")
    cur.close()
    conn.close()
    print("Connection closed")
    
    # write the errors in a text file
    if len(errors) > 0:
        with open("../errors.txt", "w") as f:
            for e in errors:
                f.write(str(e))
    

def main():
    load_pg()
if __name__ == "__main__":
    main()