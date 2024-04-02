import libsql_experimental as libsql
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def connect_db():
    """Connects to the database.

    Reads database connection details from environment variables.

    Returns:
        libsql.Connection: A connection object to the database.
    """

    url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")
    conn = libsql.connect("airports.db", sync_url=url, auth_token=auth_token)
    conn.sync()
    # conn = libsql.connect("airports.db")  # Assuming local database
    return conn


def create_table(conn, table_name, df, fill_missing_values=True):
    """Creates a table in the database.

    Args:
        conn (libsql.Connection): The connection object to the database.
        table_name (str): The name of the table to create.
        df (pandas.DataFrame): The DataFrame containing the data to be inserted.
        fill_missing_values (bool, optional): Whether to handle missing values (NaN) by
                                                replacing them with empty strings. Defaults to True.

    Raises:
        libsql.Error: If there's an error creating the table.
    """

    # Handle missing values (optional)
    if fill_missing_values:
        df.fillna('', inplace=True)

    # Get column names and data types
    columns_names = ",".join(df.columns.tolist())
    columns_types = ",".join([f"{col} TEXT" for col in df.columns])

    # Sanitize table name
    valid_chars = "_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    sanitized_table_name = "".join(
        char for char in table_name if char in valid_chars)

    # Create table query
    create_table_query = f"""create table if not exists {sanitized_table_name} ({columns_types})"""

    try:
        conn.execute(create_table_query)
        print(f"Table '{sanitized_table_name}' created successfully!")
    except libsql.Error as e:
        print(f"Error creating table: {e}")


def insert_data(conn, table_name, df):
    """Inserts data into an existing table.

    Args:
        conn (libsql.Connection): The connection object to the database.
        table_name (str): The name of the table to insert data into.
        df (pandas.DataFrame): The DataFrame containing the data to be inserted.

    Raises:
        libsql.Error: If there's an error inserting data.
    """

    try:
        cursor = conn.cursor()
        for index, row in df.iterrows():
            # Constructing the SQL INSERT query dynamically
            columns = ", ".join(row.index)
            # Ensure values are properly formatted (escaping single quotes)
            values = ", ".join([f"'{value}'" if isinstance(
                value, str) else str(value) for value in row.values])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            # print(query)
            cursor.execute(query)
            conn.commit()

        print("Data inserted successfully.")
    except libsql.Error as e:
        print(f"Error inserting data: {e}")


def sanitize_filename(filename):
    """Sanitizes a filename for database table naming.

    Args:
        filename (str): The filename to sanitize.

    Returns:
        str: The sanitized filename with invalid characters replaced by underscores.
    """

    valid_chars = "_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    sanitized_filename = "".join(
        char for char in filename if char in valid_chars)
    return sanitized_filename.split(".")[0]  # Remove extension

# initialization function: has the same role as main here.


if __name__ == "__main__":
    conn = connect_db()

    csv_folder = "csv_files"  # Path to the folder containing CSV files
    for filename in os.listdir(csv_folder):
        if filename.endswith(".csv"):
            full_path = os.path.join(csv_folder, filename)

            # Read the CSV file
            df = pd.read_csv(full_path)

            # Sanitize filename and remove ".csv" extension
            sanitized_filename = sanitize_filename(filename.split(".")[0])

            # Optional argument for handling missing values

            create_table(conn, sanitized_filename,
                         df, fill_missing_values=True)

            insert_data(conn, sanitized_filename, df)
