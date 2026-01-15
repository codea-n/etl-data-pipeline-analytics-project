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


# Query 2: Countries with only 1 border
st.header("Countries with Only 1 Border")
single_border = pd.read_sql_query("""
    SELECT country_name, border_count
    FROM country_border_counts
    WHERE border_count = 1
    ORDER BY country_name
""", conn)
st.table(single_border)

# Query 3: Average number of borders
st.header("Average Number of Borders")
avg_borders = pd.read_sql_query("""
    SELECT AVG(border_count) AS average_border_count
    FROM country_border_counts
""", conn)
st.metric("Average Borders", round(avg_borders.iloc[0,0], 2))

# Query 4: Countries above average borders
st.header("Countries Above Average Border Count")
above_avg = pd.read_sql_query("""
    SELECT country_name, border_count
    FROM country_border_counts
    WHERE border_count > (SELECT AVG(border_count) FROM country_border_counts)
    ORDER BY border_count DESC
""", conn)
st.table(above_avg)
