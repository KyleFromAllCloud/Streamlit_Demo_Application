from snowflake import connector
import streamlit as st
import pandas as pd
import numpy as np
import os
from pydantic import BaseModel
from typing import Literal
import yaml
from yaml import SafeLoader
import json
from pathlib import Path
import streamlit_authenticator as stauth
from snowflake.connector.pandas_tools import write_pandas
from pytz import country_names
from st_aggrid import AgGrid, GridUpdateMode, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from snowflake.snowpark.session import Session
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def get_forward_month_list():
    now = datetime.now()
    return [(now + relativedelta(months=i)).strftime('%b') for i in range(12)]

def get_forward_month_year_list():
    now = datetime.now()
    return [(now + relativedelta(months=i)).strftime('%b-%y') for i in range(12)]

months = get_forward_month_list()
months_years = get_forward_month_year_list()
months_years = [i.upper() for i in months_years]
cols = {months[i].upper(): months_years[i].upper() for i in range(len(months))}
cols_sorted = ['ACCOUNT', 'PORTFOLIO', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

# The code below is for the title and logo.
st.set_page_config(page_title="Dataframe with editable cells", page_icon="💾", layout="wide")
st.image(
    "https://cdn.comparably.com/27194047/l/643974_logo_hero-digital.png",
    width=50,
)
st.header('Forecast RBC Editable Dataframe')

# TYPE_MAPPINGS = {
# 'int64': 'integer',
# 'object': 'string'
# }

# arr = np.array(['X', 'B', 'C'])
# # st.write(arr)
# if arr.dtype == object:
#     print((arr == None).any())


# class Requirement:
#     @staticmethod
#     def not_null(arr: np.ndarray):
#         li = arr.tolist()
#         return not np.NaN in li


# class CsvColumn(BaseModel):
#     name: str
#     type: Literal['string', 'integer']
#     requirements: list[str] = []


# class CsvSpec(BaseModel):
#     columns: list[CsvColumn]

# with open('./login_config.yml') as file:
#     config = yaml.load(file, Loader=SafeLoader)
    
with open('./creds.json') as config_file:
    creds = json.load(config_file)
    
# authenticator = stauth.Authenticate(
#     config['credentials'],
#     config['cookie']['name'],
#     config['cookie']['key'],
#     config['cookie']['expiry_days']
# )

# name, authentication_status, username = authenticator.login('Login', 'main')

# if st.session_state["authentication_status"]:
#     authenticator.logout('Logout', 'main')
TYPE_MAPPINGS = {
    'int64': 'integer',
    'object': 'string'
}
if 'snowflake_connection' not in st.session_state:
    # connect to Snowflake
    conn = connector.connect(**st.secrets["snowflake"])
#         with open('creds.json') as f:
    connection_parameters = creds
    st.session_state.snowflake_connection = Session.builder.configs(connection_parameters).create()
    session = st.session_state.snowflake_connection
else:
    session = st.session_state.snowflake_connection
#     st.set_page_config(layout="centered", page_title="Data Editor", page_icon="🧮")
#     st.title("Snowflake Table Editor ❄️")
#     st.caption("This is a demo of the `st.experimental_data_editor`.")
if st.button('Refresh'):
    st.experimental_rerun()
col_list_trim = ['ACCOUNT', 'PORTFOLIO']
col_list = col_list_trim + months
def get_dataset():
    # load messages df
    df = session.table("FORECAST_RBC")
#     col_list = col_list + ['EXISTINGCLIENTNEWLOGO']
#     df = df[col_list]
#     df.rename(columns={"JAN": "23-JAN","FEB": "23-FEB", "MAR": "23-MAR", "APR": "23-APR", 
#                       "MAY": "23-MAY", "JUN": "23-JUN", "JUL": "23-JUL", "AUG": "23-AUG", 
#                       "SEP": "23-SEP", "OCT": "23-OCT", "NOV": "23-NOV", "DEC": "23-DEC", })
#     for i in cols:
#         df = df.with_column_renamed(i, cols[i])
#     df = df.rename(cols)
    return df
dataset = get_dataset()
# dataset_pd = session.table("FORECAST_RBC").to_pandas()
dataset_pd = pd.DataFrame(dataset.collect())
st.dataset(dataset_pd)
def format_date(x):
    return datetime.strptime(x, '%b-%y').date()
# dataset_pd = pd.melt(dataset_pd, id_vars = col_list_trim, value_vars = months_years)
# dataset_pd['variable'] = dataset_pd['variable'].map(format_date)
# dataset_pd = dataset_pd.rename(columns={"variable":"MONTH_DATE","value":"REVENUE"})
# st.dataframe(dataset_pd)
with st.form("data_editor_form"):
    st.caption("Edit the dataframe below")
    edited = st.experimental_data_editor(dataset, width=1500, num_rows="dynamic")
    submit_button = st.form_submit_button("Submit")
if submit_button:
    try:
#         for i in cols:
#             edited = edited.rename(columns={cols[i]: i})
    #     st.dataframe(edited)
    #     df.rename(columns={"A": "a", "B": "c"})
    #         st.dataframe(edited)
    #     edited = edited[[cols_sorted]]
        session.write_pandas(edited, "FORECAST_RBC", overwrite=True)
        time = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
#         edited_hist = edited
#         edited_hist = edited_hist.reindex(columns = cols_sorted)
        dataset_pd['LAST_UPDATE_DATETIME'] = time 
#         st.dataframe(dataset_pd)
#         dataset_pd = session.create_dataframe(dataset_pd)
        session.write_pandas(dataset_pd, "FORECAST_RBC_HISTORICAL", overwrite=False)
#         st.success("Table updated")
    except Exception as e:
        st.write(e)
        st.warning("Error updating table")


#     data_dict = yaml.safe_load(Path('csv_spec.yml').open('r'))
#     csv_spec = CsvSpec.parse_obj(data_dict)

#     st.write(data_dict)

df_file = pd.DataFrame()

uploaded_file = st.file_uploader('Upload Most Recent Version of Forecast RBC Numbers')
if uploaded_file is not None:
    # read csv
    df_file = pd.read_csv(uploaded_file, thousands=',')
#     df_file.columns = [x.upper() for x in df_file.columns]
#     # Validate File
#     st.header("File Validation")
#     is_valid = True
#     for col in csv_spec.columns:
#         if col.name not in df_file.columns:
#             st.error(f"Column {col.name} is missing.")
#             is_valid = False
#         else:
#             col_data = df_file[col.name]
#             if TYPE_MAPPINGS.get(str(col_data.dtype), 'XXX') != col.type:
#                 st.error(f"Column {col.name} must be of type {col.type}.")
#                 is_valid = False
#             for req in col.requirements:
#                 pass_req = getattr(Requirement, req)(col_data)
#                 if not pass_req:
#                     st.error(f"Column {col.name} failed {req} requirement.")
#                     is_valid = False

st.write(df_file)


btn_press = st.button('Submit Change')
#                           , disabled=not is_valid)

if btn_press:
    try:
#         for i in cols:
#             df_file = df_file.rename(columns={cols[i]: i})
    #     st.dataframe(edited)
    #     df.rename(columns={"A": "a", "B": "c"})
    #         st.dataframe(edited)
    #     edited = edited[[cols_sorted]]
        session.write_pandas(df_file, "FORECAST_RBC", overwrite=True)
#         dataset_file_pd = pd.DataFrame(df_file.collect())
        dataset_file_pd = pd.melt(df_file, id_vars = col_list_trim, value_vars = months_years)
        dataset_file_pd['variable'] = dataset_file_pd['variable'].map(format_date)
        dataset_file_pd = dataset_file_pd.rename(columns={"variable":"MONTH_DATE","value":"REVENUE"})
        time = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        dataset_file_pd['LAST_UPDATE_DATETIME'] = time 
        session.write_pandas(dataset_file_pd, "FORECAST_RBC_HISTORICAL", overwrite=False)
        st.success("Table updated")
    except Exception as e:
        st.warning(f"Error updating table - {e}")

       
# elif st.session_state["authentication_status"] is False:
#     st.error('Username/password is incorrect')
# elif st.session_state["authentication_status"] is None:
#     st.warning('Please enter your username and password')
