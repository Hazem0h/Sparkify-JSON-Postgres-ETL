import os
import glob
import json
from typing import List
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extensions import register_adapter
import sql_queries

def connect_to_db(config_path = "../config.json"):
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
    def adapt_np_int(np_int:np.int64):
        _INT = psycopg2.extensions.Int
        return _INT(np_int)
    register_adapter(np.int64, adapt_np_int)

def fill_artist_table(cur, path = "../data/cleaned/artists.csv"):
    df_artist = pd.read_csv(path)
    df_artist.fillna(psycopg2.extensions.AsIs("NULL"), inplace = True)
    for index, row in df_artist.iterrows():
        cur.execute(sql_queries.artist_table_insert, list(row.values))
    
def fill_song_table(cur, path = "../data/cleaned/songs.csv"):
    df_songs = pd.read_csv(path)
    df_songs.fillna(psycopg2.extensions.AsIs("NULL"), inplace = True)
    for index, row in df_songs.iterrows():
        cur.execute(sql_queries.song_table_insert, list(row.values))

def fill_time_table(cur, path = "../data/cleaned/time.csv"):
    df_times = pd.read_csv(path)
    df_times.fillna(psycopg2.extensions.AsIs("NULL"), inplace = True)
    for index, row in df_times.iterrows():
        cur.execute(sql_queries.time_table_insert, list(row.values))
    
def fill_users_table(cur, path = "../data/cleaned/users.csv"):
    df_users = pd.read_csv(path)
    df_users.fillna(psycopg2.extensions.AsIs("NULL"), inplace = True)
    for index, row in df_users.iterrows():
        cur.execute(sql_queries.user_table_insert, list(row.values))

def fill_songplays_table(cur, path = "../data/cleaned/songplays.csv"):
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
        cur.execute(sql_queries.songplay_table_insert, songplay_data)

def load_pg():
    conn, cur = connect_to_db()
    setup_adapters()
    fill_artist_table(cur)
    fill_song_table(cur)
    fill_time_table(cur)
    fill_users_table(cur)
    fill_songplays_table(cur)
    cur.close()
    conn.close()
    
if __name__ == "__main__":
    load_pg()