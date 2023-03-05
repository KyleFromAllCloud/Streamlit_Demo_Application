from snowflake import connector
import streamlit as st
import pandas as pd
import numpy as np
import os
from pydantic import BaseModel
from typing import Literal
import yaml
from pathlib import Path
import streamlit_authenticator as stauth


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


conn = connector.connect(**st.secrets["snowflake"])


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
