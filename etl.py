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

# STEP
def transform(raw_data):
#     """
#     Transform raw JSON into clean dataframe
#     """

#     rows=[]
#     for country in raw_data:
#         if "borders" in country and country["borders"]:
#             for border_country in country["borders"]:
#                 rows.append({
#                     "country_code": country["cca3"],
#                     "country_name": country["name"]["common"],
#                     "border_country_code": border_country
#                 })

#     df = pd.DataFrame(rows)
#     return df


def load(df):
    """
    Load dataframe into PostgreSQL
    """
    # 1. Connect
    conn = get_connection()
    cursor = conn.cursor()

    newly_inserted=set()

    # 2. Loop over DataFrame rows and insert
    for _,row in df.iterrows():
        try:
            cursor.execute(
                """
                INSERT INTO country_borders (country_code,country_name,border_country_code)
                VALUES (?,?,?)
                """,
                (row['country_code'],row['country_name'],row['border_country_code'])
            )
            newly_inserted.add(row['country_code'])
        except sqlite3.IntegrityError:
            pass


    
    # 3. Commit and close
    conn.commit()
    conn.close()
    return newly_inserted

def run_pipeline():
    create_table()
    raw_data = extract()
    df = transform(raw_data)
    # load(df)

    # create_border_counts_table()
    # refresh_border_counts()
    updated_countries = load(df)
    refresh_border_counts(updated_countries)


if __name__ == "__main__":
    run_pipeline()
