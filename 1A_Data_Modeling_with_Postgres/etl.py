import os
import glob
import psycopg2
import numpy as np
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Reads the song_data JSON file located in the filepath and 
    inserts relevant columns into the song and artists tables.
    
    Parameters
    ----------
    cur : Psycopg Connection object
        Cursor of the Psycopg Connection object linked to the 
        database
    filepath : str
        Location of the JSON file
        
    Returns
    -------
    None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert song record
    for i, row in df.iterrows():
        song_data = row[["song_id",\
                         "title",\
                         "artist_id",\
                         "year",\
                         "duration"]].values.tolist()
        cur.execute(song_table_insert, song_data)
    
    # insert artist record
    for i, row in df.iterrows():
        artist_data = row[["artist_id",\
                           "artist_name",\
                           "artist_location",\
                           "artist_latitude",\
                           "artist_longitude"]].values.tolist()
        cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Reads the log_data JSON file located in the filepath. The 
    function will perform the necessary data transformations and
    insert the relevant columns into the time, users and songplay 
    tables.
    
    Parameters
    ----------
    cur : Psycopg Connection object
        Cursor of the Psycopg Connection object linked to the 
        database
    filepath : str
        Location of the JSON file
        
    Returns
    -------
    None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"]=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")
    
    # insert time data records
    time_data = np.array([t.dt.values,\
                          t.dt.hour.values,\
                          t.dt.day.values,\
                          t.dt.week.values,\
                          t.dt.month.values,\
                          t.dt.year.values,\
                          t.dt.weekday.values]).T
    column_labels = ["start_time",\
                     'hour',\
                     'day',\
                     'week',\
                     'month',\
                     'year',\
                     'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId",\
                  "firstName",\
                  "lastName",\
                  "gender",\
                  "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index,\
                         pd.to_datetime(row["ts"], unit="ms"),\
                         int(row["userId"]),\
                         str(row["level"]),\
                         songid,\
                         artistid,\
                         int(row["sessionId"]),\
                         str(row["location"]),\
                         str(row["userAgent"])
                        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Retrieves all JSON files in the given filepath directory.
    For each JSON file, this function will either call the 
    process_song_file function or process_log_file function
    depending on whether the JSON file is song_data file or
    log_data file.
    
    Parameters
    ----------
    cur : Psycopg Cursor object
        Cursor of the Psycopg Connection object linked to the 
        database
    conn : Psycopg Connection object
        Psycopg Connection object linked to the 
        database
    filepath : str
        Location of the JSON file
    func : Python Function
        The relevant python function 'process_song_file' or 
        'process_log_file' to process the JSON file
        
    Returns
    -------
    None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Generates the Psycopg Connection to the Sparkify database 
    and creates the Connection's cursor. It will then call the 
    'process_data' function to perform the necessary data 
    transformations and insert the relevant data into the 
    songplays, users, songs, artists, time tables.
    
    Parameters
    ----------
    None
        
    Returns
    -------
    None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()