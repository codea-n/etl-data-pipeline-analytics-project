import altair as alt
import sqlite3
import pandas as pd
import streamlit as st



# Connect to your database
conn = sqlite3.connect("country_borders.db")

tables = pd.read_sql_query(
    "SELECT name FROM sqlite_master WHERE type='table';",
    conn
)
st.title("Country Borders Dashboard")
st.write("Tables in DB:", tables)

# Query 1: Top 10 countries by number of borders
st.header("Top 10 Countries by Number of Borders")
top10 = pd.read_sql_query("""
    SELECT country_name, border_count
    FROM country_border_counts
    ORDER BY border_count DESC
    LIMIT 10
""", conn)
st.table(top10)

chart = alt.Chart(top10).mark_bar().encode(
    x='border_count:Q',
    y=alt.Y('country_name:N', sort='-x')
)
st.altair_chart(chart, use_container_width=True)