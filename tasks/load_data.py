import os
import pandas as pd
from utils.db_utils import get_db_connection

def load_csv_to_db(folder_date):
    """Load all CSV files from the specified folder into the database."""
    folder_path = os.path.join("data", folder_date)
    conn = get_db_connection()
    cursor = conn.cursor()

    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if file_name.endswith(".csv"):
            # Load CSV file
            data = pd.read_csv(file_path, header=None)
            # Insert data into the database
            for _, row in data.iterrows():
                cursor.execute("""
                    INSERT INTO data_table (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, row.values.tolist())
            conn.commit()
    
    cursor.close()
    conn.close()
