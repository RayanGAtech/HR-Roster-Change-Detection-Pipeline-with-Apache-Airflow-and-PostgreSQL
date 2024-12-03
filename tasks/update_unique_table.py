from utils.db_utils import get_db_connection

def update_unique_table():
    """Update the unique_data_table with new unique rows."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO unique_data_table (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16)
        SELECT DISTINCT P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16
        FROM data_table
        ON CONFLICT DO NOTHING;
    """)
    conn.commit()
    cursor.close()
    conn.close()
