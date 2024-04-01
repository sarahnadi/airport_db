import libsql_experimental as libsql
import os
import pandas as pd
from dotenv import load_dotenv
import time

load_dotenv()
#download files from github and store them in a dataframe

def download_and_clean_data():
    # Download data
    df = pd.read_csv("https://raw.githubusercontent.com/davidmegginson/ourairports-data/master/airports.csv")

    # Handle NaN values (replace with empty string)
    df.fillna('', inplace=True)


    return df

# df =  pd.read_csv("https://raw.githubusercontent.com/davidmegginson/ourairports-data/master/airports.csv")
# # df.columns.tolist()
# print(df.columns.tolist())
# print(f"{col} TEXT" for col in df.columns)
# print(pd.__version__)

def connect_db():

    url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")
    # conn = libsql.connect("hello.db", sync_url=url, auth_token=auth_token)
    conn = libsql.connect("airports.db")
    # conn.sync()
    # print(conn.execute("select * from test_table;"))
    return conn

def create_table(conn, table_name, df):
    #get columns names from the dataframe
    columns_names = " ,".join(df.columns.tolist())
    columns_types = " ,".join([f"{col} TEXT" for col in df.columns])
    create_table_query = f"""create table if not exists {table_name} ({columns_types})"""

    try:
        conn.execute(create_table_query)
        print(f"Table '{table_name}' created successfully!")
    except libsql.Error as e:
        print(f"Error creating table: {e}")


# def insert_data(conn, table_name, df, max_retries=3, retry_delay=1):
#     retries = 0
#     while retries < max_retries:
#         try:
#             cursor = conn.cursor()
#             for index, row in df.iterrows():
#                 columns = ', '.join(row.index)
#                 values = ', '.join([f"'{str(value)}'" for value in row.values])
#                 query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
#                 cursor.execute(query)
#             conn.commit()
#             print("Data inserted successfully.")
#             return  # Exit the function if insertion is successful
#         except libsql.Error as e:
#             print(f"Error inserting data: {e}")
#             retries += 1
#             if retries < max_retries:
#                 print(f"Retrying in {retry_delay} seconds...")
#                 time.sleep(retry_delay)
#             else:
#                 print("Max retries reached. Aborting.")
#                 break

        
def insert_data(conn, table_name, df):
    try:
        cursor = conn.cursor()
        for index, row in df.iterrows():
            # Constructing the SQL INSERT query dynamically
            columns = ', '.join(row.index)
            # values = ', '.join([f"'{str(value)}'" for value in row.values])  # Ensure values are properly formatted
            values = ', '.join([f"'{str(value).replace("'", "''")}'" for value in row.values])

            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            # Executing the query with row values as parameters
            print(query)
            cursor.execute(query)

        conn.commit()
        print("Data inserted successfully.")
    except libsql.Error as e:
        print(f"Error inserting data: {e}")


if __name__ == "__main__":

    clean_df = download_and_clean_data() # Download and clean data
    print(clean_df.shape)
    conn = connect_db()
    create_table(conn, "airports", clean_df)
    insert_data(conn, "airports", clean_df)