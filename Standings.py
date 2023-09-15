import streamlit as st
import pandas as pd
from helpers import read_file

st.set_page_config(layout="wide")
st.title("World Cup 2023")
st.subheader("**:blue[Cric Talk Draft]** :fire:")


bucket_name = "wc-2023"
standings_file_path = "Outputs/standings_df.csv"
cumsum_file_path = "Outputs/cumsum_df.csv"

standings_df = read_file(bucket_name, standings_file_path).set_index("Standings")
cumsum_df = read_file(bucket_name, cumsum_file_path).set_index("Owner")
col1, col2 = st.columns([2, 3])

cumsum_df = cumsum_df.rename(
    columns={
        x: int(x.split("Match_")[1]) for x in cumsum_df.columns if x.startswith("Match")
    }
)

col1.subheader("Standings ")
col1.dataframe(standings_df)

col2.subheader("Draft Standings Race")
col2.line_chart(cumsum_df.T)
