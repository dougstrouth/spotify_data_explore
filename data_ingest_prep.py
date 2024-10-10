import libraries.duckdb_utils as ddb
import os
from dotenv import load_dotenv

env =load_dotenv(".env")

data_loc = os.getenv("SPOTIFY_DATA_LOC")
print(data_loc)
# Create a file path using the data path
file_name = 'data/*.json'
full_path = os.path.join(data_loc, file_name)
print(full_path)


def create_and_append_json_to_duckdb(folder_path):
  """
  Iterates through a folder of JSON files, creates a DuckDB table from the first file,
  and appends data from the remaining files.

  Args:
    folder_path: The path to the folder containing the JSON files.
  """

  file_list = [f for f in os.listdir(folder_path) if f.endswith('.json')]
  first_file = os.path.join(folder_path, file_list[0])

  # Create the table using the first JSON file
  sql_create = f"""
    CREATE TABLE json_data AS
    SELECT * FROM read_json_objects('{first_file}');
  """

  # Append data from the rest of the JSON files
  sql_append = """
    INSERT INTO json_data
    SELECT * FROM read_json_objects(?);
  """

  # This is where you would execute the SQL statements in DuckDB
  # For example, using the DuckDB Python API:
  #   conn = duckdb.connect()
  #   conn.execute(sql_create)
  #   for file in file_list[1:]:
  #     conn.execute(sql_append, [os.path.join(folder_path, file)])
  #   conn.close()

  # I'm returning the SQL statements here since I don't know how you're
  # connecting to DuckDB
  return sql_create, sql_append, file_list[1:]
