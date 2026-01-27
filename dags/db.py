import sqlite3
from config import DB_FILE

def get_connection():
    """
    Create and return a SQLite connection
    """
    return sqlite3.connect(DB_FILE)

def create_table():
    """
    Create a table if it does not exist
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS country_borders;")
    
    sql = """
    CREATE TABLE IF NOT EXISTS country_borders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country_code TEXT,
        country_name TEXT,
        border_country_code TEXT,
        CONSTRAINT unique_borders UNIQUE(country_code,border_country_code)
    );
    """
    
    cursor.execute(sql)
    conn.commit()
    conn.close()

def insert_rows(rows):
    """
    Insert transformed rows into database
    rows: list of tuples (country_code, country_name, border_country_code)
    """
    if not rows:
        return  # nothing to insert
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.executemany(
        """
        INSERT INTO country_borders (country_code, country_name, border_country_code)
        VALUES (?, ?, ?)
        """,
        rows
    )
    
    conn.commit()
    conn.close()

def create_border_counts_table():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS country_border_counts (
            country_code TEXT PRIMARY KEY,
            country_name TEXT,
            border_count INTEGER
        );
    """)

    conn.commit()
    conn.close()

def refresh_border_counts(updated_countries):
    if not updated_countries:
        return

    conn = get_connection()
    cursor = conn.cursor()

    for country_code in updated_countries:
        cursor.execute(
            """
            INSERT OR REPLACE INTO country_border_counts (
                country_code,
                country_name,
                border_count
            )
            SELECT
                country_code,
                country_name,
                COUNT(*) AS border_count
            FROM country_borders
            WHERE country_code = ?
            GROUP BY country_code, country_name;
            """,
            (country_code,)
        )

    conn.commit()
    conn.close()
