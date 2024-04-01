import libsql_experimental as libsql
import os
import pandas as pd
from dotenv import load_dotenv
import time

load_dotenv()


def download_and_clean_data():
    # Download data
    df = pd.read_csv("https://raw.githubusercontent.com/davidmegginson/ourairports-data/master/airport-frequencies.csv")

    # Handle NaN values (replace with empty string)
    df.fillna('', inplace=True)

    return df


def connect_db():

    url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")
    # conn = libsql.connect("hello.db", sync_url=url, auth_token=auth_token)
    conn = libsql.connect("airports.db")
    # conn.sync()
    # print(conn.execute("select * from test_table;"))
    return conn


def create_table(conn, table_name, df):
    # Get column names from the dataframe
    columns_names = ",".join(df.columns.tolist())
    columns_types = ",".join([f"{col} TEXT" for col in df.columns])

    # Sanitize table name (replace invalid characters with underscore)
    valid_chars = "_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    sanitized_table_name = "".join(char for char in table_name if char in valid_chars)

    create_table_query = f"""create table if not exists {sanitized_table_name} ({columns_types})"""

    try:
        conn.execute(create_table_query)
        print(f"Table '{sanitized_table_name}' created successfully!")
    except libsql.Error as e:
        print(f"Error creating table: {e}")


def insert_data(conn, table_name, df):
    try:
        cursor = conn.cursor()
        for index, row in df.iterrows():
            # Constructing the SQL INSERT query dynamically
            columns = ", ".join(row.index)
            # values = ', '.join([f"'{str(value)}'" for value in row.values])  # Ensure values are properly formatted
            values = ", ".join([f"'{str(value).replace("'", "''")}'" for value in row.values])

            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            # Executing the query with row values as parameters
            print(query)
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
    sanitized_filename = "".join(char for char in filename if char in valid_chars)
    return sanitized_filename


if __name__ == "__main__":

    clean_df = download_and_clean_data()  # Download and clean data
    print(clean_df.shape)
    conn = connect_db()

    # Get filename (assuming the script is in the same directory as the CSV)
    filename = os.path.basename("https://raw.githubusercontent.com/davidmegginson/ourairports-data/master/airport_frequencies.csv")

    # Sanitize filename
    # sanitized_filename = sanitize_filename(filename)
    sanitized_filename = sanitize_filename(filename.split(".")[0])

    create_table(conn, sanitized_filename, clean_df)
    insert_data(conn, sanitized_filename, clean_df)
