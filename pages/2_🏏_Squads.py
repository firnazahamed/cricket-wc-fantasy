import streamlit as st
import pandas as pd
from helpers import get_client, read_file

client = get_client()
bucket_name = "wc-2023"

squads = sorted(
    [
        blob.name.strip("Squads/").strip("_squad.csv")
        for blob in client.list_blobs(bucket_name, prefix="Squads")
    ],
    reverse=True,
)

option = st.selectbox("Select match id", squads)

squad_df = read_file(bucket_name, f"Squads/{option}_squad.csv")
st.subheader("Squad")
st.dataframe(squad_df)
