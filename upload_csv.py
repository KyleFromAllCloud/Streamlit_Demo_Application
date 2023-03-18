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
from snowflake.snowpark import Session

# The code below is for the title and logo.
# st.set_page_config(page_title="Dataframe with editable cells", page_icon="💾")
# st.image(
#     "https://emojipedia-us.s3.dualstack.us-west-1.amazonaws.com/thumbs/240/apple/325/floppy-disk_1f4be.png",
#     width=100,
# )

with open('./login_config.yml') as file:
    config = yaml.load(file, Loader=SafeLoader)
    
with open('./creds.json') as config_file:
    creds = json.load(config_file)
    
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

name, authentication_status, username = authenticator.login('Login', 'main')

if st.session_state["authentication_status"]:
    authenticator.logout('Logout', 'main')
    TYPE_MAPPINGS = {
        'int64': 'integer',
        'object': 'string'
    }
#     if 'snowflake_connection' not in st.session_state:
#         # connect to Snowflake
#         conn = connector.connect(**st.secrets["snowflake"])
# #         with open('creds.json') as f:
#         connection_parameters = creds
#         st.session_state.snowflake_connection = Session.builder.configs(connection_parameters).create()
#         session = st.session_state.snowflake_connection
#     else:
#         session = st.session_state.snowflake_connection
# #     st.set_page_config(layout="centered", page_title="Data Editor", page_icon="🧮")
# #     st.title("Snowflake Table Editor ❄️")
# #     st.caption("This is a demo of the `st.experimental_data_editor`.")
#     def get_dataset():
#         # load messages df
#         df = session.table("STREAMLIT_ENTRY_DEMO")
#         return df
#     dataset = get_dataset()
#     with st.form("data_editor_form"):
#         st.caption("Edit the dataframe below")
#         edited = st.experimental_data_editor(dataset, use_container_width=True, num_rows="dynamic")
#         submit_button = st.form_submit_button("Submit")
#     if submit_button:
#         try:
#             session.write_pandas(edited, "STREAMLIT_ENTRY_DEMO", overwrite=True)
#             st.success("Table updated")
#         except:
#             st.warning("Error updating table")
#     if st.button('Refresh'):
#         st.experimental_rerun()
        
    TYPE_MAPPINGS = {
    'int64': 'integer',
    'object': 'string'
    }

    arr = np.array(['X', 'B', 'C'])
    # st.write(arr)
    if arr.dtype == object:
        print((arr == None).any())


    class Requirement:
        @staticmethod
        def not_null(arr: np.ndarray):
            li = arr.tolist()
            return not np.NaN in li


    class CsvColumn(BaseModel):
        name: str
        type: Literal['string', 'integer']
        requirements: list[str] = []


    class CsvSpec(BaseModel):
        columns: list[CsvColumn]


    data_dict = yaml.safe_load(Path('csv_spec.yml').open('r'))
    csv_spec = CsvSpec.parse_obj(data_dict)


    conn = connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account='baa92216.us-east-1',
        role='STREAMLIT_DEVELOPER',
        warehouse='WH_STREAMLIT_DEMO'
    )


    st.title('Upload CSV Demo')
    st.header("File Specification")
    st.write(data_dict)

    df = pd.DataFrame()

    uploaded_file = st.file_uploader('Upload a file')
    if uploaded_file is not None:
        # read csv
        df = pd.read_csv(uploaded_file)

    # Validate File
    st.header("File Validation")
    is_valid = True
    for col in csv_spec.columns:
        if col.name not in df.columns:
            st.error(f"Column {col.name} is missing.")
            is_valid = False
        else:
            col_data = df[col.name]
            if TYPE_MAPPINGS.get(str(col_data.dtype), 'XXX') != col.type:
                st.error(f"Column {col.name} must be of type {col.type}.")
                is_valid = False
            for req in col.requirements:
                pass_req = getattr(Requirement, req)(col_data)
                if not pass_req:
                    st.error(f"Column {col.name} failed {req} requirement.")
                    is_valid = False

    st.write(df)


    btn_press = st.button('Submit Change', disabled=not is_valid)

    if btn_press:
        uploaded_cols = df.columns.to_list()
        st.write(uploaded_cols)

        if 'RequiredColumn' not in uploaded_cols:
            st.write("Failed - Missing column 'RequiredColumn'")
        else:
            st.write("Loaded!")

       
elif st.session_state["authentication_status"] is False:
    st.error('Username/password is incorrect')
elif st.session_state["authentication_status"] is None:
    st.warning('Please enter your username and password')
