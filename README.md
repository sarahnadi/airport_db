# airport_db

1. **Connects to the database:** It calls the `connect_db` function to establish a connection with the database using credentials stored in environment variables file (.env) .
2. **Loops through CSV files:** It iterates through the files in the specified `csv_folder` (default: "csv_files") using `os.listdir`.
3. **Processes only CSV files:** It checks if the filename ends with ".csv" using `filename.endswith(".csv")`.
4. **Builds the full path:** It constructs the absolute path to the CSV file using `os.path.join`.
5. **Reads CSV data:** It reads the data from the CSV file into a pandas DataFrame using `pd.read_csv`.
6. **Sanitizes filename:** It calls the `sanitize_filename` function to create a valid table name from the filename.
7. **Creates table:** It calls the `create_table` function to create a table in the database with the sanitized name,
using the DataFrame columns and data types (optionally handling missing
values).
8. **Inserts data:** It calls the `insert_data` function to insert the data from the DataFrame into the newly created table.
9. **Closes connection:** After processing all files, it closes the database connection using `conn.close`.
10. **Prints confirmation:** It prints a message indicating successful database initialization.
