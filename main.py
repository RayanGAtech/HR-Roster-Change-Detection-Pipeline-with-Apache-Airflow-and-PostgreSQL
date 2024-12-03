import os
import csv
import psycopg2
from datetime import datetime, timedelta

# Database setup (PostgreSQL)
db_name = "FP_DB"
db_user = "postgres"
db_password = "postgres"
db_host = "localhost"  # or your database host
db_port = "5432"  # default port for PostgreSQL

# Create a connection to the PostgreSQL database
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
cursor = conn.cursor()

def generate_custom_headers(num_columns):
    """Generate custom column names (P1, P2, P3, ...)"""
    return [f"P{i+1}" for i in range(num_columns)]

def create_table_from_csv(num_columns):
    """Create a table dynamically with custom column names (P1, P2, P3, ...)."""
    # Generate custom headers like P1, P2, P3, ...
    custom_headers = generate_custom_headers(num_columns)
    
    # Define columns and data types for each column (we'll assume TEXT for simplicity)
    columns = ", ".join([f"{header} TEXT" for header in custom_headers])
    
    # Define the SQL query to create the table
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            {columns}
        );
    """
    
    # Execute the query to create the table
    cursor.execute(create_table_query)
    conn.commit()

def insert_csv_to_db(file_path):
    """Insert data from a CSV file into the database."""
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            
            # Check if the file is empty
            if not csvfile.readable():
                print(f"Skipping empty file: {file_path}")
                return
            
            # Reset the pointer to the beginning of the file
            csvfile.seek(0)
            
            # Read the first row to determine the number of columns (headers)
            try:
                headers = next(reader)
            except StopIteration:
                print(f"Skipping empty CSV file (no data): {file_path}")
                return

            # If headers are found, proceed
            if headers:
                # Create the table using custom column names
                create_table_from_csv(len(headers))
                
                # Insert each row into the table
                for row in reader:
                    # Ensure the row matches the number of columns in the header
                    if len(row) == len(headers):
                        insert_query = f"""
                            INSERT INTO {table_name} ({', '.join(generate_custom_headers(len(headers)))})
                            VALUES ({', '.join(['%s'] * len(headers))});
                        """
                        cursor.execute(insert_query, row)
                conn.commit()
            else:
                print(f"Skipping file with no headers: {file_path}")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

def get_data_folders():
    """Get the path of data folders for yesterday and the day before yesterday."""
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    day_before_yesterday = today - timedelta(days=2)
    
    # Format dates as YYYYMMDD
    yesterday_folder = yesterday.strftime('%Y%m%d')
    day_before_yesterday_folder = day_before_yesterday.strftime('%Y%m%d')
    
    return yesterday_folder, day_before_yesterday_folder

def process_data_folders():
    """Process CSV files in yesterday's and the day before yesterday's data folders."""
    # Get paths for yesterday and the day before yesterday's folders
    yesterday_folder, day_before_yesterday_folder = get_data_folders()
    
    # Define base data folder
    base_folder = 'data'

    # List all CSV files in both the yesterday's and the day before yesterday's folders
    for folder in [yesterday_folder, day_before_yesterday_folder]:
        folder_path = os.path.join(base_folder, folder)
        
        # Check if the folder exists
        if os.path.exists(folder_path):
            print(f"Processing files in: {folder_path}")
            
            # Loop through all CSV files in the folder
            for filename in os.listdir(folder_path):
                if filename.endswith('.csv'):
                    file_path = os.path.join(folder_path, filename)
                    print(f"Processing file: {file_path}")
                    insert_csv_to_db(file_path)
        else:
            print(f"Folder {folder_path} does not exist. Skipping.")

# Set your table name
table_name = "data_table"  # Set your table name here

# Process data folders (yesterday and the day before yesterday)
process_data_folders()

# Close the database connection
cursor.close()
conn.close()
