import streamlit as st
import pandas as pd
from helpers import read_file

bucket_name = "summer-is-coming-2023"
score_df = read_file(bucket_name, "Outputs/score_df.csv").set_index("Owner")
st.header("Match wise player points")
st.dataframe(score_df)

sum_df = read_file(bucket_name, "Outputs/sum_df.csv").set_index("Owner")
st.header("Match aggregate points")
st.dataframe(sum_df)

cumsum_df = read_file(bucket_name, "Outputs/cumsum_df.csv").set_index("Owner")
st.header("Cumulative points")
st.dataframe(cumsum_df)
