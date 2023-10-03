import streamlit as st
import pandas as pd
from settings import owner_team_dict
from helpers import read_file
from st_aggrid import AgGrid

bucket_name = "wc-2023"

st.set_page_config(layout="wide")
for owner in sorted(owner_team_dict.keys()):
    st.header(owner)
    trade_df = read_file(bucket_name, f"Transfers/{owner}.csv").set_index("S.no")

    st.dataframe(trade_df)
    # AgGrid(trade_df)
