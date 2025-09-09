import requests
import pandas as pd
import os
import country_converter as coco
import io
from azure.storage.blob import BlobServiceClient

BASE = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"

# Sample Countries to test
COUNTRIES = ["United States", "Israel", "Italy"]

# Main indicators we will use for all countries
INDICATORS = {
    "NY.GDP.MKTP.CD": "gdp_usd",
    "SP.POP.TOTL": "population",
    "MS.MIL.XPND.GD.ZS": "mil_exp_pct_gdp",
    "MS.MIL.XPND.CD": "mil_exp_usd",
}


# We will soon pass in other years dynamically when we containerize script
START_YEAR = int(os.getenv("START_YEAR", '1989'))
END_YEAR = int(os.getenv("END_YEAR", '1991'))


# Helper Functions
def add_human_readable_columns(df):
    """Add formatted string columns for display"""
    if 'gdp_usd' in df.columns:
        df['gdp_string'] = df['gdp_usd'].apply(lambda x: human_readable(x) if pd.notna(x) else None)
    if 'population' in df.columns:
        df['population_string'] = df['population'].apply(lambda x: human_readable(x) if pd.notna(x) else None)
    return df


def merge_indicators(dataframes):
    """Merge multiple indicator DataFrames"""
    if not dataframes:
        return pd.DataFrame()

    merged = dataframes[0]
    for df in dataframes[1:]:
        merged = pd.merge(merged, df, on=["country", "iso3", "year"], how="outer")

    return merged.sort_values("year").reset_index(drop=True)


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

def get_wb_code(full_country):
    try:
        return coco.convert(full_country, to='ISO3')
    except:
        return None



def worldbank_api_request(wb_code, indicator_code, start_year, end_year):
    url = BASE.format(country=wb_code, indicator=indicator_code)
    params = {"format": "json", "date": f"{start_year}:{end_year}", "per_page": 20000}

    try:
        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"API request failed for {wb_code}, {indicator_code}: {e}")
        return None


def parse_wb_response(payload, indicator_name):
    if not payload or len(payload) < 2:
        return pd.DataFrame()

    rows = payload[1] if payload[1] else []

    return pd.DataFrame([
        {
            "country": row["country"]["value"],
            "iso3": row["countryiso3code"],
            "year": int(row["date"]),
            indicator_name: row["value"],
        }
        for row in rows if row.get("value") is not None
    ])


def fetch_country_data(country_name, start_year, end_year):
    """Fetch all indicators for a single country"""
    wb_code = get_wb_code(country_name)
    if not wb_code:
        print(f"Could not convert {country_name} to World Bank code")
        return pd.DataFrame()

    indicator_dfs = []

    for indicator_code, colname in INDICATORS.items():
        payload = worldbank_api_request(wb_code, indicator_code, start_year, end_year)
        df = parse_wb_response(payload, colname)

        if df.empty:
            print(f"No data for {country_name} - {colname}")
            continue

        indicator_dfs.append(df)

    if not indicator_dfs:
        print(f"No data found for {country_name}")
        return pd.DataFrame()

    merged = merge_indicators(indicator_dfs)
    return add_human_readable_columns(merged)




# Azure storage related functions
def upload_to_blob(df, blob_name, container_name="worldbank-data"):
    # Get connection string
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if not connection_string:
        print("No Azure storage connection string found")
        return False

    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Convert DataFrame to Parquet in memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # Upload to blob
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=f"processed/{blob_name}"
        )

        blob_client.upload_blob(parquet_buffer.getvalue(), overwrite=True)
        print(f"Successfully uploaded {blob_name} to {container_name}")
        return True

    except Exception as e:
        print(f"Failed to upload to blob storage: {e}")
        return False


def generate_blob_filename(start_year, end_year):
    """Generate standardized filename for blob storage"""
    return f"worldbank/worldbank_data_{start_year}_{end_year}.parquet"


# Replace the end of your script (after the RUN comment) with this:
# ---------------- RUN ----------------
frames = [fetch_country_data(c, START_YEAR, END_YEAR) for c in COUNTRIES]

# Filter out empty DataFrames
frames = [f for f in frames if not f.empty]

if frames:
    df = pd.concat(frames).sort_values(["year"]).reset_index(drop=True)

    print(f"Processed {len(df)} rows for {len(frames)} countries")
    print(df.head(30))

    # Upload to Azure Blob Storage
    blob_filename = generate_blob_filename(START_YEAR, END_YEAR)
    success = upload_to_blob(df, blob_filename)

    if success:
        print(f"Data successfully saved to blob storage as {blob_filename}")
    else:
        print("Failed to save to blob storage")

else:
    print("No data found for any countries")


