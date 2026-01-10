import sqlite3
from config import DB_FILE

def get_connection():
    """
    Create and return a SQLite connection
    """
    return sqlite3.connect(DB_FILE)

