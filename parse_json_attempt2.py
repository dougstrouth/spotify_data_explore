import json


def read_json_from_file(file_path):
  """
  Reads JSON data from a .json file.

  Args:
      file_path (str): The path to the .json file.

  Returns:
      dict: A Python dictionary containing the parsed JSON data.
           Returns None if there is an error reading or parsing the file.
  """
  try:
    with open(file_path, 'r') as f:
      data = json.load(f)
    return data
  except (FileNotFoundError, json.JSONDecodeError) as e:
    print(f"Error reading or parsing JSON file: {e}")
    return None

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

        info_sql = [f"""
            INSERT INTO info ("generated_on", "slice", "version")
            VALUES ('{data['info']['generated_on']}'::TIMESTAMP, '{data['info']['slice']}', '{data['info']['version']}')
            ON CONFLICT ("generated_on") DO NOTHING;
        """]

        playlists_sql = []
        tracks_sql = []
        playlist_tracks_sql = []

        for playlist in data['playlists']:
            playlists_sql.append(f"""
                INSERT INTO playlists (pid, "name", collaborative, "modified_at", num_tracks, num_albums,
                                       num_followers, num_edits, duration_ms, num_artists)
                VALUES ({playlist['pid']}, '{playlist['name']}', '{playlist['collaborative']}', {playlist['modified_at']}::TIMESTAMP,
                        {playlist['num_tracks']}, {playlist['num_albums']}, {playlist['num_followers']},
                        {playlist['num_edits']}, {playlist['duration_ms']}, {playlist['num_artists']})
                ON CONFLICT (pid) DO NOTHING;
            """)
            for track in playlist['tracks']:
                tracks_sql.append(f"""
                    INSERT INTO tracks ("track_uri", "track_name", "artist_name", "artist_uri", "album_uri", "album_name", duration_ms)
                    VALUES ('{track['track_uri']}', '{track['track_name']}', '{track['artist_name']}',
                            '{track['artist_uri']}', '{track['album_uri']}', '{track['album_name']}', {track['duration_ms']})
                    ON CONFLICT ("track_uri") DO NOTHING;
                """)
                playlist_tracks_sql.append(f"""
                    INSERT INTO playlist_tracks (pid, "track_uri", pos)
                    VALUES ({playlist['pid']}, '{track['track_uri']}', {track['pos']})
                    ON CONFLICT (pid, "track_uri") DO NOTHING;
                """)

        return {
            'info_sql': info_sql,
            'playlists_sql': playlists_sql,
            'tracks_sql': tracks_sql,
            'playlist_tracks_sql': playlist_tracks_sql
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
  for statement in sql_statements['info_sql']:
      print(statement)

  print("\nSQL statements for 'playlists' table:")
  for statement in sql_statements['playlists_sql']:
      print(statement)

  print("\nSQL statements for 'tracks' table:")
  for statement in sql_statements['tracks_sql']:
      print(statement)

  print("\nSQL statements for 'playlist_tracks' table:")
  for statement in sql_statements['playlist_tracks_sql']:
      print(statement)
