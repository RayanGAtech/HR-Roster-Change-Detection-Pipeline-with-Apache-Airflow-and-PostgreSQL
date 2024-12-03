from utils.db_utils import get_db_connection

def clean_duplicates():
    """Remove duplicates from the main table."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE data_table_temp AS
        SELECT DISTINCT * FROM data_table;

        DROP TABLE data_table;

        ALTER TABLE data_table_temp RENAME TO data_table;
    """)
    conn.commit()
    cursor.close()
    conn.close()
