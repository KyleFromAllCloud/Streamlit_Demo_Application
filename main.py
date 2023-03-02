from snowflake import connector
import streamlit as st
import pandas as pd
import datetime


conn = connector.connect(
    **st.secrets["snowflake"], client_session_keep_alive=True
)


def get_data():
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM STREAMLIT_DEMO.DEMO.BUDGET")
        return cur.fetch_pandas_all()

df = get_data()

st.title('BIO Budget Widget')
st.subheader('Raw data')


st.write(df)
selected_indices = st.selectbox('Select rows:', df.index)
selected_rows: pd.DataFrame = df.loc[selected_indices]
st.write('### Selected Rows', selected_rows)

yr = int(selected_rows['YEAR'])
mo = int(selected_rows['MONTH'])
budget = int(selected_rows['BUDGET'])

st.write('### Selected Year', int(selected_rows['YEAR']))
st.write('### Selected Month', int(selected_rows['MONTH']))
new_budget = st.slider('Budget', 0, 1_000_000, int(selected_rows['BUDGET']), step=5_000)

btn_press = st.button('Submit Change')

if btn_press:
    if (abs(new_budget - budget) / budget) > 0.20:
        st.write(f"Wow, that is a big change. Are you sure about that? {budget:,} -> {new_budget:,}")

    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE STREAMLIT_DEMO.DEMO.BUDGET
            SET BUDGET = {new_budget}
            WHERE YEAR = {yr} AND MONTH = {mo}
        """)

        cur.execute(f"""
            INSERT INTO STREAMLIT_DEMO.DEMO.BUDGET_LOG VALUES
            (
                {yr},
                {mo},
                {new_budget},
                CURRENT_TIMESTAMP
            )
        """)
        df = get_data()
