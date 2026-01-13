import sqlite3

def inspect_db(db_file, table_name="country_borders", limit=5):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Check table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print("Tables:", cursor.fetchall())

    # Fetch first N rows
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit};")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    # Example 2: count total border relationships
    cursor.execute("""
        SELECT COUNT(*) FROM country_borders;
    """)
    print("Total border pairs:", cursor.fetchone()[0])
    conn.close()

if __name__ == "__main__":
    inspect_db("country_borders.db", limit=10)
