import os
import glob
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import List, Dict

def get_files(filepath:str) -> List[str]:
    """returns all json files in the directory tree under the filepath

    Arguments:
        filepath -- the root path

    Returns:
        a list of json filepaths under the root path
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def build_dataframe(dir: str) -> pd.DataFrame:
    """Builds a single dataframe of all json files under the provided directory

    Arguments:
        dir -- The directory of JSON files we wish to load into a single dataframe

    Returns:
        df: -- The concatenated dataframe
    """
    files = get_files(dir)
    df = pd.DataFrame()
    for dir in files:
        df = pd.concat([df, pd.read_json(dir, lines = True)])
    return df


def fix_songs_artists(df_songs_artists: pd.DataFrame) -> pd.DataFrame:
    """Performs data cleaning on the songs/artists data
    """
    df_songs_artists = df_songs_artists.copy() # I don't want the functions to mutate the original object. To maintain abstraction
    df_songs_artists["year"] = df_songs_artists["year"].replace({0: np.nan})
    df_songs_artists["artist_location"] = df_songs_artists["artist_location"].replace({"": np.nan})
    return df_songs_artists

def save_artist_data(df_songs_artists: pd.DataFrame, path = "../data/cleaned/artists.csv", index = False):
    """Saves artist data, duh
    """
    artist_columns = ["artist_id", "artist_name", "artist_location", "artist_longitude", "artist_latitude"]
    df_artists = df_songs_artists[artist_columns].copy()
    df_artists.drop_duplicates(inplace = True)
    df_artists.to_csv(path, index = index)

def save_songs_data(df_songs_artists: pd.DataFrame, path = "../data/cleaned/songs.csv", index = False):
    """Saves the songs data
    """
    song_columns = ["song_id", "title", "artist_id", "year", "duration"]
    df_songs = df_songs_artists[song_columns].copy()
    df_songs.to_csv(path, index = index)
    
def fix_user_data(df_logs: pd.DataFrame) -> pd.DataFrame:
    """Fixes the issues in the users data in the log files
    """
    df_logs = df_logs.copy()
    df_logs["userId"] = df_logs["userId"].replace("", np.nan).astype(np.float64)
    df_logs = df_logs.sort_values(by = "ts")
    return df_logs

def save_users_data(df_logs: pd.DataFrame, path = "../data/cleaned/users.csv", index = True):
    """save the users data to a csv file, duh
    """
    grouped = df_logs.groupby("userId").agg("last")
    user_cols = ["firstName", "lastName", "gender", "level"]
    df_users = grouped[user_cols]
    df_users.to_csv(path, index = index)


def extract_time_data(df_logs: pd.DataFrame) -> Dict[str, pd.Series]:
    """Extracts date/time information from the dataframe (the hour, the day, the week, the month,...)
    """
    session_datetime = pd.to_datetime(df_logs["ts"], unit = "ms")
    hour = session_datetime.dt.hour
    day = session_datetime.dt.day
    weekday = session_datetime.dt.weekday
    week = session_datetime.dt.isocalendar().week
    month = session_datetime.dt.month
    year = session_datetime.dt.year
    time_data_dict = {
        "ts": df_logs["ts"].copy(),
        "hour":hour,
        "day": day,
        "weekday": weekday,
        "week": week,
        "month": month,
        "year": year
    }
    return time_data_dict

def save_time_data(time_data_dict: Dict[str, pd.Series], path = "../data/cleaned/time.csv", index = False):
    """Saves the time data into a csv file
    """
    time_df = pd.DataFrame(time_data_dict)
    time_df.drop_duplicates(inplace = True)
    time_df.to_csv(path, index = index)

def save_songplay_data(df_logs: pd.DataFrame):
    """Saves songplay data into a csv file
    """
    songplay_cols = ["sessionId", "ts", "song", "length", "artist", "userId", "level", "location", "userAgent"]
    df_songplays = df_logs[df_logs["page"] == "NextSong"].copy()
    df_songplays = df_songplays[songplay_cols]
    df_songplays.to_csv("../data/cleaned/songplays.csv", index = False)

def extract_and_transform():
    """Extracts the data from the JSON files, transforms them, and saves them into csv files.
    """
    #### Songs Data
    df_songs_artists = build_dataframe("../data/raw/song_data")
    print("loaded song files")
    df_songs_artists = fix_songs_artists(df_songs_artists)
    save_artist_data(df_songs_artists)
    save_songs_data(df_songs_artists)
    print("saved songs and artist data")
    
    #### log data
    # 1. users
    df_logs = build_dataframe("../data/raw/log_data")
    print("loaded log files")
    df_logs = fix_user_data(df_logs)
    save_users_data(df_logs)
    print("saved users data")
    # 2. time
    time_data_dict = extract_time_data(df_logs)
    save_time_data(time_data_dict)
    print("saved time data")
    # 3. songplay
    save_songplay_data(df_logs)
    print("saved songplay data")

def main():
    if not os.path.exists("../data/cleaned"):
        os.makedirs("../data/cleaned")
        
    extract_and_transform()

if __name__ == "__main__":
    main()