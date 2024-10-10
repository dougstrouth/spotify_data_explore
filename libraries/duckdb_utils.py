import logging
import duckdb
import glob
import os
from general_utilities import setup_logging

setup_logging()


def execute_sql_from_file(sql_file, folder_path, duckdb_file):
    # Read the SQL file content
    with open(sql_file, "r") as file:
        sql_content = file.read()

    # Replace the placeholder with actual path
    sql_content = sql_content.format(path=folder_path)

    # Connect to DuckDB
    conn = duckdb.connect(duckdb_file)

    # Execute the SQL script
    conn.execute(sql_content)

    # Close the connection
    conn.close()


def query_duckdb_metadata(duckdb_file_path, schema="check_csv"):
    """Query DuckDB's information_schema.columns view and return a DataFrame."""
    try:
        con = duckdb.connect(duckdb_file_path)
        query = f"""
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        """
        df = con.execute(query).fetchdf()
        logging.info("Queried DuckDB metadata.")
        csv_name = schema+"_metadata.csv"
        # Save the DataFrame to a CSV file
        metadata_csv_path = os.path.join(
            os.path.dirname(duckdb_file_path), csv_name
        )
        df.to_csv(metadata_csv_path, index=False)
        logging.info(f"Metadata saved to {metadata_csv_path}")
        return df
    except Exception as e:
        logging.error(f"Failed to query DuckDB metadata: {e}")
        raise
    finally:
        con.close()


def create_pbp_from_csvs(duckdb_file, schema_name, folder_path):
    """Create tables in DuckDB from CSV files stored in folders with specific format."""

    conn = duckdb.connect(duckdb_file)
    conn.execute(
        f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
    )  # List all subdirectories (i.e., table folders) in the given folder_path
    table_folders = [
        d for d in glob.glob(os.path.join(folder_path, "*")) if os.path.isdir(d)
    ]

    for table_folder in table_folders:
        # Extract table name from the folder name
        table_name = os.path.basename(table_folder)

        # Construct the pattern to include all CSV files within this folder
        pattern = os.path.join(table_folder, "*.csv")

        # Create SQL statement to read all CSV files in the folder and create a table
        if table_name == "play_by_play":
            # For the 'play_by_play' table, specify column types explicitly
            sql = f"""
            DROP TABLE IF EXISTS {schema_name}.{table_name};

            CREATE TABLE {schema_name}.{table_name} AS
            SELECT * FROM read_csv('{pattern}', 
                                   union_by_name=true,
                                   types={{'time_of_day': 'TIME'}});
            """
        elif table_name == "weekly_rosters":
            # For the 'weekly_rosters' table, specify column types explicitly
            sql = f"""
            DROP TABLE IF EXISTS {schema_name}.{table_name};

            CREATE TABLE {schema_name}.{table_name} AS
            SELECT * FROM read_csv('{pattern}', 
                                   union_by_name=true,
                                   types={{'jersey_number': 'VARCHAR'}});
            """
        else:
            # For other tables, no specific column types are set
            sql = f"""
            DROP TABLE IF EXISTS {schema_name}.{table_name};

            CREATE TABLE {schema_name}.{table_name} AS
            SELECT * FROM read_csv('{pattern}', 
                                   union_by_name=true);
            """

        # Execute SQL statement
        try:
            conn.execute(sql)
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"Failed to create table {table_name}: {e}")

    conn.close()



def create_tables_from_csvs(duckdb_file, schema_name, folder_path):
    """Create tables in DuckDB from CSV files directly within the provided folder."""

    # Construct the pattern to search for CSV files in the specified folder only
    pattern = os.path.join(folder_path, "*.csv")
    csv_files = glob.glob(pattern)

    conn = duckdb.connect(duckdb_file)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    for csv_file in csv_files:
        # Extract table name from the CSV file name
        table_name = os.path.splitext(os.path.basename(csv_file))[0]

        # Create SQL statement to read CSV and create a table
        sql = f"""
        DROP TABLE IF EXISTS {schema_name}.{table_name};

        CREATE TABLE {schema_name}.{table_name} AS
        SELECT * FROM read_csv('{csv_file}', union_by_name=true);
        """

        # Execute SQL statement
        try:
            conn.execute(sql)
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"Failed to create table {table_name}: {e}")

    conn.close()


folder_path = (
    "/Users/dougstrouth/Documents/Code/datasets/sports/NFL/raw_data/play_by_play"
)
duckdb_file = (
    "/Users/dougstrouth/Documents/Code/datasets/sports/NFL/raw_data/nfl_data.duckdb"
)
schema = "play_by_play"
query_duckdb_metadata(duckdb_file, schema)


# Define the SQL file containing the table creation script
sql_file = "play_by_play_setup.sql"
