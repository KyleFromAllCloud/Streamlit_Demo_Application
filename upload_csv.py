from snowflake import connector
import streamlit as st
import pandas as pd
import numpy as np
import os
from pydantic import BaseModel
from typing import Literal
import yaml
from yaml import SafeLoader
from pathlib import Path
import streamlit_authenticator as stauth
from snowflake.connector.pandas_tools import write_pandas
from pytz import country_names
from st_aggrid import AgGrid, GridUpdateMode, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder

# The code below is for the title and logo.
# st.set_page_config(page_title="Dataframe with editable cells", page_icon="💾")
# st.image(
#     "https://emojipedia-us.s3.dualstack.us-west-1.amazonaws.com/thumbs/240/apple/325/floppy-disk_1f4be.png",
#     width=100,
# )

with open('./login_config.yml') as file:
    config = yaml.load(file, Loader=SafeLoader)
    
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


#     conn = connector.connect(**st.secrets["snowflake"])

    @st.experimental_memo
    def load_data():
        tot_sql = "SELECT * FROM DEV_EDW_PSTG.DEMO_SCHEMA.STREAMLIT_ENTRY_DEMO;"

        cur = conn.cursor().execute(tot_sql)
        df_editor = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])

        df = st.experimental_data_editor(df_editor)
        return df


    @st.experimental_memo
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode("utf-8")


    def execute_query(conn, df_sel_row, table_name):
        if not df_sel_row.empty:
            conn.cursor().execute(
                "CREATE OR REPLACE TABLE "
                f"{table_name}(ID NUMBER, RADIUS FLOAT)"
            )
            write_pandas(
                conn=conn,
                df=df_sel_row,
                table_name=table_name,
                database="DEV_EDW_PSTG",
                schema="DEMO_SCHEMA",
                quote_identifiers=False,
            )


    # Initialize connection.
    # Uses st.experimental_singleton to only run once.
    @st.experimental_singleton
    def init_connection():
        return connector.connect(**st.secrets["snowflake"])
    conn = init_connection()
    df = load_data()
    st.title("Dataframe with editable cells")
    st.write("")
    st.markdown(
        """This is a demo of a dataframe with editable cells, powered by 
    [streamlit-aggrid](https://pypi.org/project/streamlit-aggrid/). 
    You can edit the cells by clicking on them, and then export 
    your selection to a `.csv` file (or send it to your Snowflake DB!)"""
    )
    st.write("")
    st.write("")
    st.subheader("① Edit and select cells")
    st.info("💡 Hold the `Shift` (⇧) key to select multiple rows at once.")
    st.caption("")
    gd = GridOptionsBuilder.from_dataframe(df)
    gd.configure_pagination(enabled=True)
    gd.configure_default_column(editable=True, groupable=True)
    gd.configure_selection(use_checkbox=False)
    gridoptions = gd.build()
    grid_table = AgGrid(
        df,
        gridOptions=gridoptions,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme="material",
    )
    sel_row = grid_table["selected_rows"]

    st.subheader(" ② Check your selection")

    st.write("")

    df_sel_row = pd.DataFrame(sel_row)
    csv = convert_df(df_sel_row)
    if not df_sel_row.empty:
        st.write(df_sel_row)
    st.download_button(
        label="Download to CSV",
        data=csv,
        file_name="results.csv",
        mime="text/csv",
    )
    st.write("")
    st.write("")

    st.subheader("③ Send to Snowflake DB ❄️")

    st.write("")
    table_name = st.text_input("Pick a table name", "YOUR_TABLE_NAME_HERE", help="No spaces allowed")
    run_query = st.button(
        "Add to DB", on_click=execute_query, args=(conn, df_sel_row, table_name)
    )
    if run_query and not df_sel_row.empty:
        st.success(
            f"✔️ Selection added to the `{table_name}` table located in the `STREAMLIT_DB` database."
        )
        st.snow()

    if run_query and df_sel_row.empty:
        st.info("Nothing to add to DB, please select some rows")

    st.title('Upload CSV Demo')
    st.header("File Specification")
    st.write(data_dict)

    df_file = pd.DataFrame()
#     tot_sql = "SELECT * FROM DEV_EDW_PSTG.DEMO_SCHEMA.STREAMLIT_ENTRY_DEMO;"

#     cur = conn.cursor().execute(tot_sql)
#     df_editor = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    
#     edited_df = st.experimental_data_editor(df_editor)
    
    uploaded_file = st.file_uploader('Upload a file')
    if uploaded_file is not None:
        # read csv
        df_file = pd.read_csv(uploaded_file)

    # Validate File
    st.header("File Validation")
    is_valid = True
    for col in csv_spec.columns:
        if col.name not in df_file.columns:
            st.error(f"Column {col.name} is missing.")
            is_valid = False
        else:
            col_data = df_file[col.name]
            if TYPE_MAPPINGS.get(str(col_data.dtype), 'XXX') != col.type:
                st.error(f"Column {col.name} must be of type {col.type}.")
                is_valid = False
            for req in col.requirements:
                pass_req = getattr(Requirement, req)(col_data)
                if not pass_req:
                    st.error(f"Column {col.name} failed {req} requirement.")
                    is_valid = False

    st.write(df_file)


    btn_press = st.button('Submit Change', disabled=not is_valid)

    if btn_press:
        uploaded_cols = df_file.columns.to_list()
        st.write(uploaded_cols)

        if 'RequiredColumn' not in uploaded_cols:
            st.write("Failed - Missing column 'RequiredColumn'")
        else:
            success, nchunks, nrows, _ = write_pandas(conn, df_file, 'stauth_demo')
            st.write("Loaded!")

elif st.session_state["authentication_status"] is False:
    st.error('Username/password is incorrect')
elif st.session_state["authentication_status"] is None:
    st.warning('Please enter your username and password')
