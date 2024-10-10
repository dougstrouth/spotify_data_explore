import logging
import duckdb
import glob
import os



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


def query_duckdb_metadata(duckdb_file_path, schema):
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
