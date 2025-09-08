import requests
import pandas as pd
from src.containers.utils import human_readable
BASE = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"


COUNTRIES = ["USA", "MEX", "GBR"]
INDICATORS = {
    "NY.GDP.MKTP.CD": "gdp_usd",
    "SP.POP.TOTL": "population",
    "MS.MIL.XPND.GD.ZS": "mil_exp_pct_gdp",
    "MS.MIL.XPND.CD": "mil_exp_usd",
}

START, END = 2012, 2023

def fetch_indicator(country, start, end):
    merged = None
    for code, colname in INDICATORS.items():
        url = BASE.format(country=country, indicator=code)
        params = {"format": "json", "date": f"{start}:{end}", "per_page": 20000}
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()
        rows = payload[1] if isinstance(payload, list) and len(payload) > 1 else []

        df = pd.DataFrame([
            {
                "country": row["country"]["value"],
                "iso3": row["countryiso3code"],
                "year": int(row["date"]),
                colname: row["value"],
            }
            for row in rows if row.get("value") is not None
        ])

        # merge indicator onto main DataFrame
        if merged is None:
            merged = df
        else:
            merged = pd.merge(merged, df, on=["country", "iso3", "year"], how="outer")

    merged['gdp_string'] = merged['gdp_usd'].apply(human_readable)
    merged['population_string'] = merged['population'].apply(human_readable)
    return merged.sort_values("year").reset_index(drop=True)


# ---------------- RUN ----------------
frames = [fetch_indicator(c, START, END) for c in COUNTRIES]
df = pd.concat(frames).sort_values(["year"]).reset_index(drop=True)


df.to_csv("gdp_countries.csv", index=False)
