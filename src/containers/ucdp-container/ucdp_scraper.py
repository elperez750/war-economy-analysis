import requests, time, json
import pandas as pd
import os
import io
from datetime import datetime
from azure.storage.blob import BlobServiceClient


def main():

    GW_CODES = [645, 700, 775, 540, 666]

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
        "StartDate": "1989-01-01",
        "EndDate": "1991-12-31",
        "pagesize": 1000
    }

    while True:
        r = requests.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()

        events = payload.get("Result", [])
        all_events.extend(events)

        next_url = payload.get("NextPageUrl")
        if not next_url:
            break
        url, params = next_url, None  # NextPageUrl already includes query params
        time.sleep(0.2)  # gentle pacing

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
    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_service = BlobServiceClient.from_connection_string(connection_string)

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
