import streamlit as st
import pandas as pd
from helpers import read_file

bucket_name = "wc-2023"
unsold_df = read_file(bucket_name, "Unsold_players.csv")
st.header("Unsold Players")
# st.dataframe(unsold_df)
st.table(unsold_df)
