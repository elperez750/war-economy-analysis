import requests, time, json
import pandas as pd
import os
import io
from datetime import datetime
from azure.storage.blob import BlobServiceClient

START_YEAR = int(os.getenv("START_YEAR", '1989'))
END_YEAR = int(os.getenv("END_YEAR", '1991'))
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", None)



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


def main():
    print(f"DEBUG: START_YEAR = {START_YEAR}")
    print(f"DEBUG: END_YEAR = {END_YEAR}")
    print(f"DEBUG: Date range will be {START_YEAR}-01-01 to {END_YEAR}-12-31")

    GW_CODES = get_country_codes(AZURE_STORAGE_CONNECTION_STRING)

    all_events = []

    for country in GW_CODES:
        print(f"\n=== Starting retrieval for country code: {country} ===")
        scrape_country(country, all_events)


    # Building the dataframe
    df = pd.DataFrame(all_events)
    agg = cast_and_aggregate(df)
    save_to_blobs(df, agg)



def scrape_country(country, all_events):
    BASE_URL = "https://ucdpapi.pcr.uu.se/api/gedevents/25.1"


    params = {
        "Country": country,
        "StartDate": f"{START_YEAR}-01-01",
        "EndDate": f"{END_YEAR}-12-31",
        "pagesize": 1000
    }

    url = BASE_URL

    while True:
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json()

            events = payload.get("Result", [])
            all_events.extend(events)

            next_url = payload.get("NextPageUrl")
            if not next_url:
                break
            url, params = next_url, None  # NextPageUrl already includes query params
            time.sleep(1)  # gentle pacing
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error {e.response.status_code}: {e}")
            if e.response.status_code == 400:
                print("Bad request - possibly rate limited or invalid date range")
                time.sleep(5)  # Wait 5 seconds before retrying
                continue
            else:
                raise

    print("\n--- Retrieval Complete ---")
    print(f"Total events retrieved: {len(all_events)}")






def cast_and_aggregate(df):

    # Parse dates & cast numerics safely
    df["date_start"] = pd.to_datetime(df.get("date_start"), errors="coerce")
    for col in ["best", "low", "high", "deaths_civilians"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0).astype(int)

    # Aggregate to Country–Year
    agg = (
        df.groupby(["country_id", "country", "year"], dropna=False)
          .agg(
              ged_events          = ("id", "count"),
              ged_events_fatal    = ("best", lambda x: (x > 0).sum()),
              ged_deaths_best     = ("best", "sum"),
              ged_deaths_low      = ("low", "sum"),
              ged_deaths_high     = ("high", "sum"),
              ged_deaths_civilians= ("deaths_civilians", "sum"),
              ged_dyads           = ("dyad_new_id", "nunique"),
              ged_state_events    = ("type_of_violence", lambda x: (x == 1).sum()),
              ged_nonstate_events = ("type_of_violence", lambda x: (x == 2).sum()),
              ged_onesided_events = ("type_of_violence", lambda x: (x == 3).sum()),
          )
          .reset_index()
          .sort_values(["country_id", "year"])
    )

    return agg


def save_to_blobs(df_raw, df_agg):
    blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

    # Save raw data
    raw_buffer = io.BytesIO()
    df_raw.to_parquet(raw_buffer, index=False)
    raw_buffer.seek(0)

    blob_client = blob_service.get_blob_client(
        container="test-data",
        blob=f"raw/events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    )
    blob_client.upload_blob(raw_buffer.getvalue(), overwrite=True)
    print("✅ Raw data saved to blob storage")

    # Save aggregated data
    agg_buffer = io.BytesIO()
    df_agg.to_parquet(agg_buffer, index=False)
    agg_buffer.seek(0)

    blob_client = blob_service.get_blob_client(
        container="test-data",
        blob=f"processed/aggregated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    )
    blob_client.upload_blob(agg_buffer.getvalue(), overwrite=True)
    print("✅ Aggregated data saved to blob storage")


if __name__ == "__main__":
    main()
