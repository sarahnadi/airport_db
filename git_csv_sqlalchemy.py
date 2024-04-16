# import libsql_experimental as libsql
import os
import pandas as pd
from dotenv import load_dotenv
from dagster import asset
import requests
from urllib.parse import urljoin
import os
import hashlib
from dagster import asset
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError 

load_dotenv()


def connect_db():
    """Connects to the database.
    """
    server = os.getenv("SERVER")
    # url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")

    # conn = libsql.connect("airports.db", sync_url= url, auth_token=auth_token)  # Assuming local database
    # conn.sync()
    # sql alchemy
    url = f"sqlite+libsql://{server}?authToken={auth_token}&secure=true"
    engine = create_engine(url)
    print(url)
    # conn = libsql.connect("airports.db")
    # return conn
    return engine

def create_table(conn, table_name, df, fill_missing_values=True):
    """Creates a table in the database.
    """

    # Handle missing values (optional)
    if fill_missing_values:
        df.fillna('', inplace=True)

    # Get column names and data types
    columns_names = ",".join(df.columns.tolist())
    columns_types = ",".join([f"{col} TEXT" for col in df.columns])

    # Sanitize table name
    valid_chars = "_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    sanitized_table_name = "".join(char for char in table_name if char in valid_chars)

    # Create table query
    create_table_query = f"""create table if not exists {sanitized_table_name} ({columns_types})"""

    try:
        conn.execute(create_table_query)
        print(f"Table '{sanitized_table_name}' created successfully!")
    except libsql.Error as e:
        print(f"Error creating table: {e}")


# def insert_data(conn, table_name, df):
#     """Inserts data into an existing table.
#     """
#     try:
#         # Direct insertion using pandas.to_sql
#         df.to_sql(table_name, conn, index=False, if_exists='replace')
#         print(f"Data inserted into table '{table_name}' successfully!")
#     except Exception as e:
#         print(f"Error inserting data: {e}")

def insert_data(conn, table_name, df, chunksize=5000):
    """Inserts data into an existing table in chunks.
    """
    chunk_number = 1 
    start_index = 0
    while start_index < len(df):
        end_index = min(start_index + chunksize, len(df))
        data_chunk = df.iloc[start_index:end_index]  # Select data chunk

        try:
            data_chunk.to_sql(table_name, conn, index=False, if_exists='replace')
            print(f"Data chunk number {chunk_number} inserted into table '{table_name}' successfully!")
            # chunk_number += 1
        except Exception as e:

            print(f"Error inserting data chunk {chunk_number}: {e}")
        finally:
            chunk_number += 1

        start_index = end_index


        



def sanitize_filename(filename):
    """Sanitizes a filename for database table naming.
    """

    valid_chars = "_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    sanitized_filename = "".join(char for char in filename if char in valid_chars)
    return sanitized_filename.split(".")[0]  # Remove extension
#####################################################
def download_csv(url, local_filename, last_modified_header=None):
    """Downloads a CSV file from the given URL, optionally checking for updates.
    """

    if os.path.exists(local_filename):
        if last_modified_header is None:
            # No previous download information, assume update required
            return download_file(url, local_filename)

        # Check if local file is up-to-date based on Last-Modified header
        with open(local_filename, 'rb') as f:
            local_hash = hashlib.md5(f.read()).hexdigest()

        response = requests.head(url)
        remote_hash = response.headers.get('Last-Modified', None)

        if local_hash == remote_hash:
            # Local file is up-to-date
            print(f"{local_filename} is already up-to-date.")
            return True
        else:
            # Local file needs update
            print(f"{local_filename} has been updated on remote server. Downloading...")
            return download_file(url, local_filename)
    else:
        # Local file doesn't exist, download it
        return download_file(url, local_filename)


def download_file(url, local_filename):
    """Downloads a file from the given URL."""

    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(local_filename, 'wb') as f:
        for chunk in response.iter_content(1024):
            f.write(chunk)

    print(f"{local_filename} downloaded successfully.")
    return True

@asset
def csv_download_initialization():
    """Downloads CSV files from the GitHub link and checks for updates."""

    base_url = "https://raw.githubusercontent.com/davidmegginson/ourairports-data/main/"  # the actual GitHub link
    local_dir = "csv_files"  # You can change this directory

    os.makedirs(local_dir, exist_ok=True)  # Create the directory if it doesn't exist

    csv_filenames = ['airports.csv', 'countries.csv', 'navaids.csv', 'regions.csv',
                     'runways.csv', 'airport-frequencies.csv', 'airport-comments.csv']

    last_modified_headers = {}  # Dictionary to store last modified headers

    for filename in csv_filenames:
        url = urljoin(base_url, filename)
        local_filename = os.path.join(local_dir, filename)

        # Check for updates or download if file doesn't exist
        downloaded = download_csv(url, local_filename, last_modified_headers.get(filename))

        if downloaded:
            # Update last modified header for future checks
            response = requests.head(url)
            last_modified_headers[filename] = response.headers.get('Last-Modified', None)


#####################################################
@asset(deps=[csv_download_initialization])
def initialize_database():
    csv_folder="csv_files"
    conn = connect_db()

    for filename in os.listdir(csv_folder):
        if filename.endswith(".csv"):
            full_path = os.path.join(csv_folder, filename)

            # Read the CSV file
            df = pd.read_csv(full_path)

            # Sanitize filename and remove ".csv" extension
            sanitized_filename = sanitize_filename(filename.split(".")[0])

            # Create table with optional handling of missing values
            # create_table(conn, sanitized_filename, df, fill_missing_values=True)

            # Insert data into the created table
            insert_data(conn, sanitized_filename, df)

    # conn.close()  # Close the database connection after processing
    print("Database initialization complete!")




if __name__ == "__main__":
    csv_download_initialization()
    initialize_database("csv_files")
