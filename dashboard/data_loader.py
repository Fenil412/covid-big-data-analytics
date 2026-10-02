"""Load processed analytics from MongoDB Atlas or local Parquet (Spark pipeline output)."""

from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import streamlit as st

METRIC_DEFINITIONS = {
    "total_cases": "Sum of new_confirmed from Google COVID-19 Open Data (country-level rows).",
    "total_deaths": "Sum of new_deceased.",
    "total_recoveries": "Sum of new_recovered.",
    "active_cases": "Latest cumulative confirmed − deceased − recovered (≥ 0).",
    "total_people_vaccinated": "Sum of new_persons_vaccinated: persons receiving one or more vaccine doses.",
    "total_vaccine_doses_administered": "Sum of new_vaccine_doses_administered: individual vaccine doses administered.",
    "total_vaccinated": "Legacy output field; its source definition is unavailable, so it is not classified as people or doses.",
    "cases_per_million": "total_confirmed / (population / 1e6) from demographics.csv join.",
    "deaths_per_million": "total_deceased / (population / 1e6).",
    "vaccinations_per_hundred": "total_vaccine_doses_administered / (population / 100).",
}


@st.cache_resource
def get_mongo_client():
    from pymongo import MongoClient

    uri = os.getenv("MONGODB_ATLAS_URI", "")
    if not uri:
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=6000, tls=True)
        client.admin.command("ping")
        return client
    except Exception:
        return None


@st.cache_data(ttl=300)
def load_collection(collection_name: str) -> tuple[pd.DataFrame, str]:
    client = get_mongo_client()
    if client is not None:
        try:
            db = client[os.getenv("MONGODB_DATABASE", "covid_analytics")]
            docs = list(db[collection_name].find({}, {"_id": 0}))
            if docs:
                return pd.DataFrame(docs), "MongoDB Atlas"
        except Exception:
            pass

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(root, "output", collection_name)
    if os.path.exists(path):
        try:
            return pd.read_parquet(path), "Local Parquet (Spark output)"
        except Exception:
            pass
    return pd.DataFrame(), "None"


def normalize_vaccination_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "total_vaccine_doses_administered" not in df.columns and "total_vaccine_doses" in df.columns:
        df = df.copy()
        df["total_vaccine_doses_administered"] = df["total_vaccine_doses"]
    return df


def apply_territory_filter(df: pd.DataFrame, hide_territories: bool) -> pd.DataFrame:
    """Compatibility shim: retain every source region, including territories."""
    return df.reset_index(drop=True)


def merge_country_vaccination(country_df: pd.DataFrame, vacc_df: pd.DataFrame) -> pd.DataFrame:
    if country_df.empty:
        return country_df
    out = country_df.copy()
    if not vacc_df.empty and "location_key" in vacc_df.columns:
        vacc_cols = [
            "location_key", "total_people_vaccinated", "total_vaccine_doses_administered",
            "peak_daily_vaccination_doses", "peak_daily_vaccine_doses_administered",
        ]
        avail = [c for c in vacc_cols if c in vacc_df.columns]
        if avail:
            vaccination_values = vacc_df[avail].drop_duplicates("location_key").copy()
            overlap = [c for c in avail if c != "location_key" and c in out.columns]
            temporary_names = {c: f"{c}__vaccination" for c in overlap}
            vaccination_values = vaccination_values.rename(columns=temporary_names)
            out = out.merge(vaccination_values, on="location_key", how="left")
            for column in overlap:
                out[column] = out[column].combine_first(out[f"{column}__vaccination"])
                out = out.drop(columns=[f"{column}__vaccination"])
    if "population" in out.columns and "total_confirmed" in out.columns:
        pop_m = out["population"] / 1_000_000
        out["cases_per_million"] = out["total_confirmed"] / pop_m.replace(0, pd.NA)
        out["deaths_per_million"] = out["total_deceased"] / pop_m.replace(0, pd.NA)
        people = out.get("total_people_vaccinated")
        if people is not None:
            out["people_vaccinated_per_hundred"] = people / (out["population"] / 100)
        doses = out.get("total_vaccine_doses_administered")
        if doses is not None:
            out["vaccinations_per_hundred"] = doses / (out["population"] / 100)
    return out


def compute_kpis(global_df: pd.DataFrame, country_df: pd.DataFrame, daily_df: pd.DataFrame) -> dict:
    """Dynamic KPIs — prefer global_metrics row from Spark pipeline."""
    kpis = {
        "total_cases": None,
        "total_deaths": None,
        "total_recoveries": None,
        "active_cases": None,
        "total_vaccine_doses_administered": None,
        "total_people_vaccinated": None,
        "countries_regions": 0,
        "latest_data_date": None,
    }

    if not global_df.empty:
        row = global_df.iloc[0]
        for key in ("total_cases", "total_deaths", "total_recoveries", "active_cases_worldwide", "total_vaccine_doses_administered", "total_people_vaccinated", "latest_data_date", "countries_reporting"):
            if key in row and pd.notna(row[key]):
                if key == "active_cases_worldwide":
                    kpis["active_cases"] = row[key]
                elif key == "countries_reporting":
                    kpis["countries_regions"] = int(row[key])
                else:
                    kpis[key.replace("active_cases_worldwide", "active_cases")] = row[key]

    if not country_df.empty:
        if kpis["total_cases"] is None and "total_confirmed" in country_df.columns:
            kpis["total_cases"] = country_df["total_confirmed"].sum()
        if kpis["total_deaths"] is None and "total_deceased" in country_df.columns:
            kpis["total_deaths"] = country_df["total_deceased"].sum()
        if kpis["total_recoveries"] is None and "total_recovered" in country_df.columns:
            kpis["total_recoveries"] = country_df["total_recovered"].sum()
        if kpis["active_cases"] is None and "active_cases" in country_df.columns:
            kpis["active_cases"] = country_df["active_cases"].sum()
        if kpis["total_people_vaccinated"] is None and "total_people_vaccinated" in country_df.columns:
            kpis["total_people_vaccinated"] = country_df["total_people_vaccinated"].sum()
        if kpis["total_vaccine_doses_administered"] is None and "total_vaccine_doses_administered" in country_df.columns:
            kpis["total_vaccine_doses_administered"] = country_df["total_vaccine_doses_administered"].sum()
        if kpis["countries_regions"] == 0 and "location_key" in country_df.columns:
            kpis["countries_regions"] = country_df["location_key"].nunique()

    if not daily_df.empty and "date" in daily_df.columns:
        daily_df = daily_df.copy()
        daily_df["date"] = pd.to_datetime(daily_df["date"])
        if kpis["latest_data_date"] is None:
            kpis["latest_data_date"] = daily_df["date"].max()

    return kpis


def filter_by_date_range(daily_df: pd.DataFrame, start, end) -> pd.DataFrame:
    if daily_df.empty or "date" not in daily_df.columns:
        return daily_df
    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df[(df["date"] >= pd.Timestamp(start)) & (df["date"] <= pd.Timestamp(end))]


def data_quality_report(dfs: dict[str, pd.DataFrame], source: str) -> dict:
    report = {"source": source, "collections": {}, "generated_at": datetime.now().isoformat()}
    for name, df in dfs.items():
        if df.empty:
            report["collections"][name] = {"rows": 0, "columns": [], "missing_pct": 100.0}
            continue
        missing = df.isnull().sum().sum()
        cells = df.shape[0] * df.shape[1]
        report["collections"][name] = {
            "rows": len(df),
            "columns": list(df.columns),
            "missing_cells": int(missing),
            "missing_pct": round(100 * missing / max(cells, 1), 2),
            "duplicates": int(df.duplicated().sum()),
        }
    return report
