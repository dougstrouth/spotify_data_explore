import logging
import os
import sys
import inspect
import pandas as pd
from IPython import get_ipython
import duckdb
from ydata_profiling import ProfileReport


def setup_logging(log_file=None):
    if log_file is None:
        if "ipykernel" in sys.modules:
            try:
                ipython = get_ipython()
                if ipython is not None:
                    # Access the current notebook path from IPython
                    notebook_name = ipython.get_parent()["content"]["name"]
                    log_file = os.path.splitext(notebook_name)[0] + "_debug.log"
                else:
                    # Fallback if IPython is not available
                    log_file = "notebook_debug.log"
            except Exception:
                # Fallback if the notebook name can't be determined
                log_file = "notebook_debug.log"
        else:
            # Get the name of the script that called setup_logging
            caller_frame = inspect.stack()[1]  # Get the caller frame
            caller_filename = caller_frame.filename
            log_file = (
                os.path.splitext(os.path.basename(caller_filename))[0] + "_debug.log"
            )

    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def clean_df_time_for_profiling(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        # Check if the column is of datetime dtype
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # Convert datetime column to string with 'yyyy-mm-dd' format
            df[col] = df[col].dt.strftime("%Y-%m-%d")
    return df


def clean_df_convert_object_for_profiling(df: pd.DataFrame) -> pd.DataFrame:
    # Iterate over columns with dtype 'object'
    for col in df.select_dtypes(include=["object"]).columns:
        # Convert each column to string
        df[col] = df[col].astype(str)
    return df


def connect_to_duckdb(duckdb_file_path: str):
    conn = duckdb.connect(duckdb_file_path)
    return conn


def sql_to_df(conn: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    df = conn.sql(sql).df()
    return df


def profiling(
    df: pd.DataFrame,
    table_name: str,
    output_type="html",
    config_file_path="/Users/dougstrouth/Documents/config_minimal.yaml",
):
    # Determine the directory of the script or notebook that triggered the function
    if "ipykernel" in sys.modules:
        try:
            ipython = get_ipython()
            if ipython is not None:
                notebook_path = ipython.get_parent()["content"]["path"]
                output_dir = os.path.join(
                    os.path.dirname(notebook_path), "profiling_reports"
                )
            else:
                output_dir = os.path.join(os.getcwd(), "profiling_reports")
        except Exception:
            output_dir = os.path.join(os.getcwd(), "profiling_reports")
    else:
        caller_frame = inspect.stack()[1]  # Get the caller frame
        caller_filename = caller_frame.filename
        output_dir = os.path.join(os.path.dirname(caller_filename), "profiling_reports")

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Generate the profile report
    profile = ProfileReport(df, title=f"{table_name}", config_file=config_file_path)

    if output_type == "html":
        html_path = os.path.join(output_dir, f"{table_name}.html")
        profile.to_file(html_path)
        return f"File written as HTML to {html_path}"
    elif output_type == "json":
        json_path = os.path.join(output_dir, f"{table_name}.json")
        profile.to_file(json_path)
        return f"File written as JSON to {json_path}"
    else:
        logging.error("Please select either 'html' or 'json' as the output type")
        return "Error: Invalid output type specified"


def get_schema_info(conn: duckdb.DuckDBPyConnection, schema: str) -> pd.DataFrame:
    """
    Retrieves information about tables and columns in a given schema from the information_schema.

    Parameters:
    - conn: The DuckDB connection object.
    - schema: The schema name to filter the information by.

    Returns:
    - A pandas DataFrame containing the information_schema data filtered by the schema.
    """
    # SQL query to get tables and columns information filtered by the given schema
    query = f"""
    SELECT 
        table_schema,
        table_name,
        column_name,
        data_type
    FROM 
        information_schema.columns
    WHERE 
        table_schema = '{schema}'
    """

    # Execute the query and return the result as a DataFrame
    df = conn.sql(query).df()
    return df
