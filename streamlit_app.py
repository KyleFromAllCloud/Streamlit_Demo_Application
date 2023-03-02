import streamlit as st
from streamlit_ace import st_ace
import pandas as pd
import snowflake.connector
import numpy as np
import altair as alt
import math

st.set_page_config(layout="wide")

# if 'queried' not in st.session_state:
#     st.session_state.queried = False

# Open a connection to Snowflake, using Streamlit's secrets management
# In real life, we’d use @st.cache or @st.experimental_memo to add caching
# @st.cache(suppress_st_warning=True)
# def query():

conn = snowflake.connector.connect(**st.secrets["snowflake"])

# Get a list of available counties from the State of California Covid Dataset
# Data set is available free here: https://app.snowflake.com/marketplace/listing/GZ1MBZAUJF
# More info on the data set: https://www.snowflake.com/datasets/state-of-california-california-covid-19-datasets/ 
counties = pd.read_sql("SELECT distinct area from open_data.vw_cases ORDER BY area asc;", conn)

# # Ask the user to select a county
# option = st.selectbox('Select an area:', counties)

option = add_selectbox = st.sidebar.selectbox(
  'Select an area:', counties
)

st.markdown(
"""
<style>
[data-testid="stSidebar"][aria-expanded="true"] > div:first-child {
width: 250px;
}
[data-testid="stSidebar"][aria-expanded="false"] > div:first-child {
width: 250px;
margin-left: -250px;
}
</style>
""",
unsafe_allow_html=True
)

one,two = st.columns(2)

with one:
  sqlquery = st_ace(language="sql",value="""
  SELECT date day, area, SUM(cases) CASES
  FROM open_data.vw_cases
  WHERE
    date between '2021-01-01' and '2021-12-31'--date > dateadd('days', -365, '2022-12-31')
    and area_type = 'County'
  GROUP BY day, area
  HAVING SUM(cases) > 0
  """)

# Query the data set to get the case counts for the last 30 days for the chosen county
# cases = pd.read_sql("""
#  SELECT area, date day, SUM(cases) CASES
#  FROM open_data.vw_cases
#  WHERE
#   date > dateadd('days', -30, current_date())
# GROUP BY area, day
# ORDER BY day asc;"""
# , conn, params={"option":option}
# )
cases = pd.read_sql(sqlquery, conn, params={"option":option})
cases = cases.set_index(['DAY'])

# test = query()

# if st.session_state.queried == False:
#     st.session_state.queried = True

with two:
  st.dataframe(cases)

first,second = st.columns(2)

#with first:
# Write text
content = st_ace(language="json",value="""
{
  "data": {
    "values": [
      {"DAY": "1644624000000", "CASES": 0},
      {"DAY": "1644710400000", "CASES": 20}
    ]
  },
  "mark": "line",
  "encoding": {
    "tooltip": [
      {"field": "DAY", "type": "temporal"},
      {"field": "CASES", "type": "quantitative"}
    ],
    "x": {
      "field": "DAY",
      "scale": {"type": "utc"},
      "type": "temporal"
    },
    "y": {"field": "CASES", "type": "quantitative"}
  }
}
""")

alt_chart = alt.Chart.from_json(content)

cases.reset_index(inplace=True)

alt_chart.data = cases


#with second:
st.altair_chart(alt_chart, use_container_width = True)

ncol = st.sidebar.number_input("Items", 1, 100, 1)
wcol = st.sidebar.number_input("Columns", 1, 100, 1)

cols = st.columns(wcol)

for i in range(ncol):
    col = cols[i%wcol]
    with col:
      col.selectbox(f"Input # {i}",[1,2,3], key=i)
      st.altair_chart(alt_chart, use_container_width = True)
