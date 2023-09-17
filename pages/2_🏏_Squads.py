import streamlit as st
import pandas as pd
from helpers import get_client, read_file

client = get_client()
bucket_name = "summer-is-coming-2023"

for blob in client.list_blobs(bucket_name, prefix="Squads"):

    # df = pd.read_csv(blob.name)
    df = read_file(bucket_name, blob.name)
    st.header(blob.name.split("Squads/")[1].strip(".csv"))
    st.dataframe(df)


@st.cache_data(ttl=600)
def load_data(sheets_url):
    csv_url = sheets_url.replace("/edit#gid=", "/export?format=csv&gid=")
    return pd.read_csv(csv_url)


# print(st.secrets["type"])
df = load_data(st.secrets["public_gsheets_url"])

st.header("Squad from GSheets")
st.dataframe(df)
