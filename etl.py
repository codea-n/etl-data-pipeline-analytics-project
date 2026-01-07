import sqlite3
import requests
import pandas as pd
from config import API_URL
from db import create_table, get_connection, insert_rows, create_border_counts_table, refresh_border_counts

def extract():
    """
    Extract data from API
    Return raw JSON
    In data engineering, extract = network I/O:
    1. Make an HTTP request
    2. Check the response
    3. Return raw JSON
    """

    # 1. Make HTTP request
    response = requests.get(API_URL)

    # 2. Check status code
    if response.status_code!=200:
        raise Exception("API request failed")

    # 3. Convert response to JSON
    data=response.json()

    # 4. Return raw JSON
    return data

