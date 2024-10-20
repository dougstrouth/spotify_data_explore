import os
import re
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from dotenv import load_dotenv

env = load_dotenv(".env")

data_loc = os.getenv("SPOTIFY_DATA_LOC")
print(data_loc)
# Create a file path using the data path
file_name = "data/"
data_path = os.path.join(data_loc, file_name)


import json


def parse_json_to_sql(json_data):
    """
    Parses JSON data and generates SQL INSERT statements for DuckDB with error handling.

    Args:
        json_data: A JSON string in the specified format.

    Returns:
        A dictionary containing lists of SQL INSERT statements for each table:
            - info_sql: List of SQL statements for the 'info' table.
            - playlists_sql: List of SQL statements for the 'playlists' table.
            - tracks_sql: List of SQL statements for the 'tracks' table.
            - playlist_tracks_sql: List of SQL statements for the 'playlist_tracks' table.
    """
    try:
        data = json.loads(json_data)

        info_sql = [
            f"""
            INSERT INTO info ("generated_on", "slice", "version")
            VALUES ('{data['info']['generated_on']}'::TIMESTAMP, '{data['info']['slice']}', '{data['info']['version']}')
            ON CONFLICT ("generated_on") DO NOTHING;
        """
        ]

        playlists_sql = []
        tracks_sql = []
        playlist_tracks_sql = []

        for playlist in data["playlists"]:
            playlists_sql.append(
                f"""
                INSERT INTO playlists (pid, "name", collaborative, "modified_at", num_tracks, num_albums,
                                       num_followers, num_edits, duration_ms, num_artists)
                VALUES ({playlist['pid']}, '{playlist['name']}', '{playlist['collaborative']}', {playlist['modified_at']}::TIMESTAMP,
                        {playlist['num_tracks']}, {playlist['num_albums']}, {playlist['num_followers']},
                        {playlist['num_edits']}, {playlist['duration_ms']}, {playlist['num_artists']})
                ON CONFLICT (pid) DO NOTHING;
            """
            )
            for track in playlist["tracks"]:
                tracks_sql.append(
                    f"""
                    INSERT INTO tracks ("track_uri", "track_name", "artist_name", "artist_uri", "album_uri", "album_name", duration_ms)
                    VALUES ('{track['track_uri']}', '{track['track_name']}', '{track['artist_name']}',
                            '{track['artist_uri']}', '{track['album_uri']}', '{track['album_name']}', {track['duration_ms']})
                    ON CONFLICT ("track_uri") DO NOTHING;
                """
                )
                playlist_tracks_sql.append(
                    f"""
                    INSERT INTO playlist_tracks (pid, "track_uri", pos)
                    VALUES ({playlist['pid']}, '{track['track_uri']}', {track['pos']})
                    ON CONFLICT (pid, "track_uri") DO NOTHING;
                """
                )

        return {
            "info_sql": info_sql,
            "playlists_sql": playlists_sql,
            "tracks_sql": tracks_sql,
            "playlist_tracks_sql": playlist_tracks_sql,
        }

    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing JSON data: {e}")
        return None


# Example usage with your JSON data (replace with your actual JSON data)
json_string = """
{
  "info": {
    "generated_on": "2017-12-03 08:41:42.057563",
    "slice": "0-999",
    "version": "v1"
  },
  "playlists": [
    {
      # ... your playlist data here
    },
    {
      # ... your playlist data here
    }
  ]
}
"""

sql_statements = parse_json_to_sql(json_string)

if sql_statements:
    print("SQL statements for 'info' table:")
    for statement in sql_statements["info_sql"]:
        print(statement)

    print("\nSQL statements for 'playlists' table:")
    for statement in sql_statements["playlists_sql"]:
        print(statement)

    print("\nSQL statements for 'tracks' table:")
    for statement in sql_statements["tracks_sql"]:
        print(statement)

    print("\nSQL statements for 'playlist_tracks' table:")
    for statement in sql_statements["playlist_tracks_sql"]:
        print(statement)


def extract_slice(filepath):
    """
    Extracts the starting numeric slice from a file path.
    """
    match = re.search(r"slice\.(\d+)-", filepath)
    if match:
        return int(match.group(1))
    return 0  # Default if no match is found


def get_files(input_dir: str):
    file_list = [f for f in os.listdir(input_dir) if f.endswith(".json")]
    file_list.sort(key=extract_slice)
    print("re_ordered: ", file_list)
    total_files = len(file_list)
    return file_list


def json_to_parquet_chunked(input_dir: str, output_file: str, chunksize=5):
    """
    Converts a directory of JSON files to a single Parquet file,
    processing in chunks to manage memory.

    Args:
        input_dir (str): Path to the directory containing JSON files.
        output_file (str): Path to the output Parquet file.
        chunksize (int): Number of JSON files to process per chunk.
    """

    file_list = [f for f in os.listdir(input_dir) if f.endswith(".json")]
    file_list.sort(key=extract_slice)
    print("re_ordered: ", file_list)
    total_files = len(file_list)

    try:
        # Check if the output file exists and get already processed files
        if os.path.exists(output_file):
            parquet_file = pq.ParquetFile(output_file)
            num_row_groups = parquet_file.num_row_groups
            processed_files = (
                num_row_groups * chunksize
            )  # Assuming each row group corresponds to a chunk
        else:
            processed_files = 0

        for i in range(processed_files, total_files, chunksize):
            dfs = []
            current_chunk = file_list[i : i + chunksize]

            for filename in current_chunk:
                filepath = os.path.join(input_dir, filename)

                try:
                    # Read the JSON file in chunks (adjust chunksize as needed)
                    for chunk in pd.read_json(filepath, lines=True, chunksize=10000):
                        dfs.append(chunk)
                except Exception as e:
                    print(f"Error reading file {filename}: {e}")
                    continue  # Skip to the next file

            # Concatenate DataFrames within the chunk
            df_chunk = pd.concat(dfs, ignore_index=True)

            # Convert to PyArrow Table
            table = pa.Table.from_pandas(df_chunk)

            # Write to Parquet file (append if it exists)
            try:
                if i == 0:
                    pq.write_table(table, output_file)
                else:
                    pq.write_table(table, output_file, append=True)
            except Exception as e:
                print(f"Error writing to Parquet file: {e}")
                break  # Stop processing if there's a write error

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# Example usage
json_to_parquet_chunked(data_path, "output.parquet")
