import pandas as pd
import os
from dotenv import load_dotenv

env =load_dotenv(".env")
print(env)
def get_data_path():
    # Get the value of the SPOTFY_DATA_LOC variable from the .env file
    data_loc = os.getenv("SPOTIFY_DATA_LOC")

    # If the variable is not set, raise an error
    if data_loc is None:
        raise ValueError("SPOTFY_DATA_LOC variable is not set")

    return data_loc

# Example usage:
data_path = get_data_path()
print(data_path)

# Create a file path using the data path
file_name = 'data/example.txt'
full_path = os.path.join(data_path, file_name)
print(full_path)
