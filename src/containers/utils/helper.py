from azure.storage.blob import BlobServiceClient
import pandas as pd
import io


def human_readable(num):
    num = float(num)  # make sure it’s numeric
    if num >= 1_000_000_000_000:   # Trillions
        return f"{num/1_000_000_000_000:.1f} T"
    elif num >= 1_000_000_000:     # Billions
        return f"{num/1_000_000_000:.1f} B"
    elif num >= 1_000_000:         # Millions
        return f"{num/1_000_000:.1f} M"
    elif num >= 1_000:             # Thousands
        return f"{num/1_000:.0f} K"
    else:
        return str(num)


def get_country_codes(connection_string):

    print(f"This is the connection string {connection_string}")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    blob_client = blob_service_client.get_blob_client(
        container='reference-data',
        blob='gw_codes.csv'
    )

    blob_data = blob_client.download_blob()
    df = pd.read_csv(io.BytesIO(blob_data.readall()))
    gw_code_list = convert_csv_to_list(df)
    return gw_code_list



def convert_csv_to_list(df):
    return list(df['StateNum'])

