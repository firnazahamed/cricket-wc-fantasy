import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import storage
from io import BytesIO

# Create API client.
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = storage.Client(credentials=credentials)

    return client


@st.cache_data(ttl=600)
def read_file(bucket_name, file_path, format="csv", sheet_name=None):
    client = get_client()
    bucket = client.bucket(bucket_name)
    content = bucket.blob(file_path).download_as_bytes()
    if format == "csv":
        df = pd.read_csv(BytesIO(content))
    elif format == "excel":
        df = pd.read_excel(BytesIO(content), sheet_name=sheet_name)

    return df


def upload_df_to_gcs(df, file_path, bucket_name):

    # Setting credentials using the downloaded JSON file
    client = storage.Client.from_service_account_json(
        json_credentials_path="credentials/cricinfo-273202-a7420ddc1abd.json"
    )
    bucket = client.get_bucket(bucket_name)
    bucket.blob(file_path).upload_from_string(
        df.to_csv(header=True, index=False), "text/csv"
    )
    # object_name_in_gcs_bucket = bucket.blob(file_path)
    # object_name_in_gcs_bucket.upload_from_filename(df)
