import libraries.duckdb_utils as ddb_u
import duckdb as ddb
import os
from dotenv import load_dotenv
import re

env = load_dotenv(".env")

data_loc = os.getenv("SPOTIFY_DATA_LOC")
print(data_loc)
# Create a file path using the data path
file_name = 'data/'
data_path = os.path.join(data_loc, file_name)
ddb_path = os.path.join(data_loc, "data/test.duckdb")



def extract_slice(filepath):
    """
    Extracts the starting numeric slice from a file path.
    """
    match = re.search(r'slice\.(\d+)-', filepath)
    if match:
        return int(match.group(1))
    return 0  # Default if no match is found

file_list = [f for f in os.listdir(folder_path) if f.endswith('.json')]
file_list.sort(key=extract_slice)
print("re_ordered: ",file_list)

def create_and_append_json_to_duckdb(ddb_path: str, folder_path: str):
    """
    Iterates through a folder of JSON files, creates a DuckDB table from the
    first file, and appends data from the remaining files.

    Args:
        folder_path: The path to the folder containing the JSON files.
    """
    file_list = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    file_list.sort(key=extract_slice)
    print("re_ordered: ",file_list)

    first_file = os.path.join(folder_path, file_list[0])
    print("first_file: ",first_file)
    # Create the table using the first JSON file
    sql_create = f"""
        CREATE TABLE main.json_data AS
        SELECT * FROM read_json_objects('{first_file}', format='auto', maximum_object_size=93554428);
    """
    conn = ddb.connect(ddb_path)
    #conn.sql(sql_create)

    for file in file_list[1:]:
        # Construct the full file path
        full_file_path = os.path.join(folder_path, file)
        sql_append = f"""
            INSERT INTO main.json_data
            SELECT * FROM read_json_objects('{full_file_path}', format='auto', maximum_object_size=93554428);
        """
        print(file)
        conn.sql(sql_append)

    conn.close()

    return file_list[1:]

create_and_append_json_to_duckdb(ddb_path, data_path)
