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
file_name = 'data/'
data_path = os.path.join(data_loc, file_name)

def extract_slice(filepath):
    """
    Extracts the starting numeric slice from a file path.
    """
    match = re.search(r'slice\.(\d+)-', filepath)
    if match:
        return int(match.group(1))
    return 0  # Default if no match is found

def json_to_parquet_chunked(input_dir, output_file, chunksize=5):
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
            processed_files = num_row_groups * chunksize  # Assuming each row group corresponds to a chunk
        else:
            processed_files = 0

        for i in range(processed_files, total_files, chunksize):
            dfs = []
            current_chunk = file_list[i: i + chunksize]

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
