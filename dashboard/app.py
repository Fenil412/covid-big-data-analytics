"""
COVID-19 Big Data Analytics Dashboard
Project: COVID-19 Big Data Analytics Platform
Author:  Aayush Savaliya (Member 3 — Analytics & NoSQL Engineer)

Interactive web dashboard built with Streamlit + Plotly.
Reads analytics results from MongoDB Atlas and displays:
  - Overview metrics
  - Top 10 affected countries
  - Daily global trends with 7-day rolling avg
  - Country comparison tool
  - Vaccination progress
  - Hospitalization burden
  - Raw data explorer

Run:
    streamlit run dashboard/app.py
"""

import os
import sys
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime
from dataclasses import dataclass, field
import re
import time
from io import BytesIO
import numpy as np
try:
    from scipy import stats as scipy_stats
except ImportError:
    scipy_stats = None

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dashboard.data_loader import (
    compute_kpis as compute_processed_kpis,
    data_quality_report,
    load_collection as load_processed_collection,
    METRIC_DEFINITIONS,
    merge_country_vaccination,
    normalize_vaccination_columns,
)
from dashboard.theme import generate_theme_css
from dotenv import load_dotenv
load_dotenv()

# ── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="COVID-19 Big Data Analytics",
    page_icon="🦠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Collection Schemas (required columns per collection) ──────────────────────
COLLECTION_SCHEMAS: dict[str, list[str]] = {
    "country_summary": [
        "location_key", "country_name", "total_confirmed", "total_deceased",
        "total_recovered", "total_vaccinated", "peak_daily_confirmed",
        "days_reported", "case_fatality_rate",
    ],
    "daily_summary": [
        "date", "global_new_confirmed", "global_new_deceased",
        "global_new_recovered",
    ],
    "vaccination_summary": [
        "location_key", "country_name", "total_vaccinated",
        "cumulative_vaccinated", "peak_daily_vaccinated", "vaccination_days",
    ],
    "hospitalization_summary": [
        "location_key", "country_name", "total_hospitalized",
        "peak_hospitalized", "avg_daily_hospitalized", "hospitalization_days",
    ],
}


# ── Validation Result ─────────────────────────────────────────────────────────
@dataclass
class ValidationResult:
    collection: str
    missing_columns: list[str] = field(default_factory=list)
    out_of_range_count: int = 0
    total_rows: int = 0
    failure_pct: float = 0.0


def get_theme_config():
    """Return Plotly layout colors for the selected persistent theme."""
    is_dark = st.session_state.get("theme_mode", "Dark") == "Dark"
    if is_dark:
        return {
            "template": "plotly_dark",
            "paper_bgcolor": "rgba(0,0,0,0)",
            "plot_bgcolor": "rgba(0,0,0,0)",
            "font_color": "#e0e0e0",
            "grid_color": "rgba(255,255,255,0.06)",
            "title_color": "#f0f0f0",
        }
    else:
        return {
            "template": "plotly_white",
            "paper_bgcolor": "rgba(0,0,0,0)",
            "plot_bgcolor": "rgba(0,0,0,0)",
            "font_color": "#2c3e50",
            "grid_color": "rgba(0,0,0,0.08)",
            "title_color": "#1a1a2e",
        }


def apply_chart_theme(fig, height=500, show_legend=False):
    """Apply consistent theming to a Plotly figure."""
    tc = get_theme_config()
    fig.update_layout(
        template=tc["template"],
        paper_bgcolor=tc["paper_bgcolor"],
        plot_bgcolor=tc["plot_bgcolor"],
        font=dict(color=tc["font_color"], family="Inter, sans-serif"),
        title_font=dict(color=tc["title_color"], size=16, family="Inter, sans-serif"),
        height=height,
        showlegend=show_legend,
        coloraxis_showscale=False,
        margin=dict(l=20, r=20, t=50, b=20),
    )
    fig.update_xaxes(gridcolor=tc["grid_color"], zeroline=False)
    fig.update_yaxes(gridcolor=tc["grid_color"], zeroline=False)
    return fig


# ── Curated Color Palettes (work in both dark & light) ────────────────────────
# Vibrant, high-contrast colors visible on any background
PALETTE_CASES    = ["#FF6B6B", "#EE5A24", "#FF4757", "#FC427B", "#E44D26",
                    "#FD7272", "#F8B739", "#FF6348", "#EB4D4B", "#E15F41"]
PALETTE_DEATHS   = ["#A29BFE", "#6C5CE7", "#786FA6", "#574B90", "#9B59B6",
                    "#8E44AD", "#BE2EDD", "#7D5FFF", "#6F1E51", "#833471"]
PALETTE_VACC     = ["#00D2D3", "#01A3A4", "#0ABDE3", "#48DBFB", "#2ED573",
                    "#26DE81", "#20BF6B", "#0FB9B1", "#1ABC9C", "#00B894"]
PALETTE_HOSP     = ["#FFA502", "#FF7F50", "#ECCC68", "#F19066", "#F5CD79",
                    "#FF6B81", "#FF4757", "#FFC312", "#F79F1F", "#EE5A24"]
PALETTE_COMPARE  = ["#FF6B6B", "#48DBFB", "#FECA57", "#00D2D3", "#A29BFE",
                    "#FF9FF3", "#54A0FF", "#5F27CD", "#01A3A4", "#EE5A24"]
COLORSCALE_CASES = [[0, "#FFEAA7"], [0.3, "#FDCB6E"], [0.6, "#E17055"], [1, "#D63031"]]
COLORSCALE_DEATH = [[0, "#DFE6E9"], [0.3, "#A29BFE"], [0.6, "#6C5CE7"], [1, "#2D3436"]]
COLORSCALE_VACC  = [[0, "#DFFBE0"], [0.3, "#55EFC4"], [0.6, "#00B894"], [1, "#006266"]]
COLORSCALE_HOSP  = [[0, "#FFEAA7"], [0.3, "#FAB1A0"], [0.6, "#E17055"], [1, "#D63031"]]
COLORSCALE_CFR   = [[0, "#FFEAA7"], [0.35, "#FDCB6E"], [0.65, "#E17055"], [1, "#C0392B"]]


# ── Custom CSS (Dark + Light Mode Support) ─────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* ─── Header Banner ─── */
    .main-header {
        background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
        padding: 2.2rem 2.5rem;
        border-radius: 18px;
        margin-bottom: 1.8rem;
        color: white;
        box-shadow: 0 10px 40px rgba(0,0,0,0.35);
        position: relative;
        overflow: hidden;
    }
    .main-header::before {
        content: '';
        position: absolute;
        top: -50%;
        right: -20%;
        width: 300px;
        height: 300px;
        background: radial-gradient(circle, rgba(255,107,107,0.15) 0%, transparent 70%);
        border-radius: 50%;
    }
    .main-header h1 {
        margin: 0;
        font-size: 2.1rem;
        font-weight: 800;
        letter-spacing: -0.5px;
        background: linear-gradient(90deg, #fff, #a29bfe);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .main-header p {
        margin: 0.5rem 0 0 0;
        opacity: 0.85;
        font-size: 0.95rem;
        font-weight: 400;
        color: #d0d0e8;
    }

    /* ─── Tech Badges ─── */
    .tech-badge {
        display: inline-block;
        padding: 5px 14px;
        border-radius: 24px;
        font-size: 0.72rem;
        font-weight: 600;
        margin-right: 6px;
        margin-bottom: 5px;
        letter-spacing: 0.3px;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .tech-badge:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
    }
    .badge-spark    { background: rgba(255,107,107,0.2); color: #FF6B6B; border: 1px solid rgba(255,107,107,0.3); }
    .badge-hadoop   { background: rgba(254,202,87,0.2);  color: #FECA57; border: 1px solid rgba(254,202,87,0.3); }
    .badge-mongo    { background: rgba(0,210,211,0.2);   color: #00D2D3; border: 1px solid rgba(0,210,211,0.3); }
    .badge-docker   { background: rgba(84,160,255,0.2);  color: #54A0FF; border: 1px solid rgba(84,160,255,0.3); }
    .badge-pyspark  { background: rgba(162,155,254,0.2); color: #A29BFE; border: 1px solid rgba(162,155,254,0.3); }
    .badge-python   { background: rgba(46,213,115,0.2);  color: #2ED573; border: 1px solid rgba(46,213,115,0.3); }

    /* ─── Section Headers ─── */
    .section-header {
        font-size: 1.35rem;
        font-weight: 700;
        margin: 1.8rem 0 1rem 0;
        padding-bottom: 0.6rem;
        border-bottom: 3px solid;
        border-image: linear-gradient(90deg, #FF6B6B, #A29BFE, #00D2D3) 1;
        letter-spacing: -0.3px;
    }

    /* ─── Info Banner ─── */
    .info-banner {
        border-radius: 12px;
        padding: 0.9rem 1.2rem;
        margin-bottom: 1rem;
        font-size: 0.85rem;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    /* ─── Tab Styling ─── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 6px;
        border-bottom: 2px solid rgba(162,155,254,0.15);
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px 10px 0 0;
        padding: 10px 22px;
        font-weight: 600;
        font-size: 0.88rem;
        transition: all 0.2s ease;
    }
    .stTabs [aria-selected="true"] {
        border-bottom: 3px solid #A29BFE;
    }

    /* ─── Metric Cards ─── */
    div[data-testid="stMetricValue"] {
        font-size: 1.7rem;
        font-weight: 700;
    }
    div[data-testid="stMetricLabel"] {
        font-weight: 600;
        font-size: 0.82rem;
        letter-spacing: 0.5px;
        text-transform: uppercase;
        opacity: 0.75;
    }

    /* ─── Dataframe styling ─── */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }

    /* ─── Download button ─── */
    .stDownloadButton > button {
        border-radius: 10px;
        font-weight: 600;
        padding: 0.5rem 1.5rem;
        transition: all 0.2s ease;
    }
    .stDownloadButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 15px rgba(0,0,0,0.15);
    }

    /* ─── Sidebar ─── */
    section[data-testid="stSidebar"] {
        border-right: 1px solid rgba(162,155,254,0.1);
    }

    /* ─── Footer ─── */
    .footer-container {
        text-align: center;
        font-size: 0.82rem;
        padding: 1.5rem 0;
        opacity: 0.7;
        line-height: 1.8;
    }
    .footer-container a {
        color: #A29BFE;
        text-decoration: none;
        font-weight: 600;
    }
    .footer-container a:hover {
        text-decoration: underline;
    }
</style>
""", unsafe_allow_html=True)


# ── MongoDB Connection & Fallback ──────────────────────────────────────────────
# Configure public DNS resolver for reliable MongoDB Atlas SRV resolution
try:
    import dns.resolver
    resolver = dns.resolver.Resolver()
    resolver.nameservers = ["8.8.8.8", "1.1.1.1", "8.8.4.4"]
    dns.resolver.default_resolver = resolver
except Exception:
    pass


@st.cache_resource
def get_mongo_client():
    """Connect to MongoDB Atlas."""
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
    """Load a collection from MongoDB Atlas with fallback to local Parquet cache."""
    return load_processed_collection(collection_name)


CONTINENT_MAP = {
    # North America
    "US": "North America", "CA": "North America", "MX": "North America", "CU": "North America",
    "DO": "North America", "HT": "North America", "JM": "North America", "CR": "North America",
    "PA": "North America", "GT": "North America", "HN": "North America", "SV": "North America",
    "NI": "North America", "BS": "North America", "BZ": "North America", "TT": "North America",
    "BB": "North America", "LC": "North America", "VC": "North America", "GD": "North America",
    "AG": "North America", "DM": "North America", "KN": "North America", "BM": "North America",
    "PR": "North America", "GL": "North America",
    # South America
    "BR": "South America", "AR": "South America", "CO": "South America", "CL": "South America",
    "PE": "South America", "EC": "South America", "VE": "South America", "BO": "South America",
    "PY": "South America", "UY": "South America", "GY": "South America", "SR": "South America",
    # Europe
    "FR": "Europe", "DE": "Europe", "IT": "Europe", "GB": "Europe", "RU": "Europe",
    "ES": "Europe", "NL": "Europe", "PL": "Europe", "UA": "Europe", "BE": "Europe",
    "SE": "Europe", "CH": "Europe", "AT": "Europe", "PT": "Europe", "GR": "Europe",
    "CZ": "Europe", "RO": "Europe", "HU": "Europe", "DK": "Europe", "SK": "Europe",
    "IE": "Europe", "NO": "Europe", "FI": "Europe", "HR": "Europe", "BG": "Europe",
    "RS": "Europe", "SI": "Europe", "LT": "Europe", "LV": "Europe", "EE": "Europe",
    "IS": "Europe", "LU": "Europe", "MT": "Europe", "CY": "Europe", "AL": "Europe",
    "BA": "Europe", "MK": "Europe", "ME": "Europe", "MD": "Europe", "BY": "Europe",
    "MC": "Europe", "LI": "Europe", "SM": "Europe", "AD": "Europe", "VA": "Europe",
    # Asia
    "IN": "Asia", "KR": "Asia", "JP": "Asia", "CN": "Asia", "TR": "Asia", "VN": "Asia",
    "ID": "Asia", "IR": "Asia", "TH": "Asia", "MY": "Asia", "PH": "Asia", "IQ": "Asia",
    "BD": "Asia", "PK": "Asia", "IL": "Asia", "SA": "Asia", "SG": "Asia", "AE": "Asia",
    "KZ": "Asia", "JO": "Asia", "LK": "Asia", "NP": "Asia", "MM": "Asia", "UZ": "Asia",
    "LB": "Asia", "QA": "Asia", "KW": "Asia", "OM": "Asia", "BH": "Asia", "AF": "Asia",
    "AZ": "Asia", "AM": "Asia", "GE": "Asia", "MN": "Asia", "KH": "Asia", "SY": "Asia",
    "YE": "Asia", "TJ": "Asia", "KG": "Asia", "LA": "Asia", "BN": "Asia", "BT": "Asia",
    "MV": "Asia", "TL": "Asia", "KP": "Asia", "PS": "Asia", "TW": "Asia", "HK": "Asia",
    # Africa
    "ZA": "Africa", "EG": "Africa", "NG": "Africa", "MA": "Africa", "ET": "Africa",
    "KE": "Africa", "GH": "Africa", "DZ": "Africa", "TN": "Africa", "UG": "Africa",
    "ZM": "Africa", "ZW": "Africa", "AO": "Africa", "MZ": "Africa", "CM": "Africa",
    "CI": "Africa", "SN": "Africa", "MG": "Africa", "SD": "Africa", "NA": "Africa",
    "RW": "Africa", "MW": "Africa", "BW": "Africa", "GA": "Africa", "MR": "Africa",
    "LY": "Africa", "SZ": "Africa", "LS": "Africa", "SO": "Africa", "TD": "Africa",
    "ML": "Africa", "BF": "Africa", "NE": "Africa", "GN": "Africa", "BJ": "Africa",
    "BI": "Africa", "SL": "Africa", "TG": "Africa", "LR": "Africa", "CG": "Africa",
    "CD": "Africa", "CF": "Africa", "DJ": "Africa", "ER": "Africa", "GM": "Africa",
    "GW": "Africa", "MU": "Africa", "SC": "Africa", "CV": "Africa", "KM": "Africa",
    # Oceania
    "AU": "Oceania", "NZ": "Oceania", "PG": "Oceania", "FJ": "Oceania", "SB": "Oceania",
    "VU": "Oceania", "WS": "Oceania", "TO": "Oceania", "FM": "Oceania", "KI": "Oceania",
    "MH": "Oceania", "PW": "Oceania", "NR": "Oceania", "TV": "Oceania", "NC": "Oceania",
}

# ── Load Data ──────────────────────────────────────────────────────────────────
country_df_raw, src1 = load_collection("country_summary")
daily_df, src2 = load_collection("daily_summary")
vacc_df_raw, src3 = load_collection("vaccination_summary")
hosp_df_raw, src4 = load_collection("hospitalization_summary")
global_df, src5 = load_collection("global_metrics")
regional_df, src6 = load_collection("regional_summary")
country_daily_df, src7 = load_collection("country_daily_summary")

# Fallback load from local output if empty
_root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if regional_df.empty:
    _reg_p = os.path.join(_root_dir, "output", "regional_summary")
    if os.path.exists(_reg_p):
        try:
            regional_df = pd.read_parquet(_reg_p)
            src6 = "Local Parquet"
        except Exception:
            pass

if global_df.empty:
    _glb_p = os.path.join(_root_dir, "output", "global_metrics")
    if os.path.exists(_glb_p):
        try:
            global_df = pd.read_parquet(_glb_p)
            src5 = "Local Parquet"
        except Exception:
            pass

if country_df_raw.empty:
    _cnt_p = os.path.join(_root_dir, "output", "country_summary")
    if os.path.exists(_cnt_p):
        try:
            country_df_raw = pd.read_parquet(_cnt_p)
            src1 = "Local Parquet"
        except Exception:
            pass

data_source = src1 if src1 != "None" else "Local Storage"

country_df = country_df_raw.reset_index(drop=True)
if "continent" not in country_df.columns and "location_key" in country_df.columns:
    country_df["continent"] = country_df["location_key"].map(CONTINENT_MAP).fillna("Other")
vacc_df = normalize_vaccination_columns(vacc_df_raw).reset_index(drop=True)
hosp_df = hosp_df_raw.reset_index(drop=True)

country_metrics_df = merge_country_vaccination(country_df, vacc_df)
if "continent" not in country_metrics_df.columns and "location_key" in country_metrics_df.columns:
    country_metrics_df["continent"] = country_metrics_df["location_key"].map(CONTINENT_MAP).fillna("Other")
if "population" not in country_metrics_df.columns:
    demographics_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dataset", "demographics.csv")
    if os.path.isfile(demographics_path):
        try:
            population_df = pd.read_csv(demographics_path, usecols=["location_key", "population"])
            population_df = population_df[population_df["location_key"].astype(str).str.len() == 2]
            country_metrics_df = country_metrics_df.merge(
                population_df.drop_duplicates("location_key"), on="location_key", how="left"
            )
        except (ValueError, OSError, pd.errors.ParserError):
            pass
if "population" in country_metrics_df.columns and "cases_per_million" not in country_metrics_df.columns:
    population_millions = pd.to_numeric(country_metrics_df["population"], errors="coerce") / 1_000_000
    if "total_confirmed" in country_metrics_df.columns:
        country_metrics_df["cases_per_million"] = pd.to_numeric(country_metrics_df["total_confirmed"], errors="coerce") / population_millions.replace(0, np.nan)
    if "total_deceased" in country_metrics_df.columns:
        country_metrics_df["deaths_per_million"] = pd.to_numeric(country_metrics_df["total_deceased"], errors="coerce") / population_millions.replace(0, np.nan)
    dose_column_for_rate = "total_vaccine_doses_administered" if "total_vaccine_doses_administered" in country_metrics_df.columns else "total_people_vaccinated"
    if dose_column_for_rate in country_metrics_df.columns:
        country_metrics_df["vaccinations_per_hundred"] = pd.to_numeric(country_metrics_df[dose_column_for_rate], errors="coerce") / (population_millions * 10_000).replace(0, np.nan)

# Convert date column
if "date" in daily_df.columns:
    daily_df["date"] = pd.to_datetime(daily_df["date"])
    daily_df = daily_df.sort_values("date")


# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <h1>🦠 COVID-19 Big Data Analytics Dashboard</h1>
    <p>Distributed analytics on 12.5M+ records using Hadoop HDFS + Apache Spark + MongoDB Atlas</p>
    <div style="margin-top: 14px;">
        <span class="tech-badge badge-spark">Apache Spark</span>
        <span class="tech-badge badge-hadoop">Hadoop HDFS</span>
        <span class="tech-badge badge-mongo">MongoDB Atlas</span>
        <span class="tech-badge badge-docker">Docker</span>
        <span class="tech-badge badge-pyspark">PySpark</span>
        <span class="tech-badge badge-python">Python</span>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🎨 Dashboard Settings")

    # Theme toggle
    theme_mode = st.radio(
        "Chart Theme",
        ["Dark", "Light"],
        index=0,
        horizontal=True,
        key="theme_mode",
        help="Switch chart colors between dark and light mode.",
    )
    st.markdown(generate_theme_css(theme_mode), unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("## 🌐 Global Filters")
    available_countries = sorted(country_df["country_name"].dropna().astype(str).unique()) if "country_name" in country_df else []
    selected_countries = st.multiselect(
        "Countries / regions",
        available_countries,
        key="global_country_filter",
        help="Leave empty to include every country and region in the processed results.",
    )
    date_bounds = None
    if not daily_df.empty and "date" in daily_df.columns:
        parsed_dates = pd.to_datetime(daily_df["date"], errors="coerce").dropna()
        if not parsed_dates.empty:
            date_bounds = (parsed_dates.min().date(), parsed_dates.max().date())
    selected_dates = None
    if date_bounds:
        selected_dates = st.date_input(
            "Date range",
            value=date_bounds,
            min_value=date_bounds[0],
            max_value=date_bounds[1],
            key="global_date_filter",
        )
    selected_aggregation = st.selectbox(
        "Historical grouping", ["Daily", "Weekly", "Monthly"], key="history_grouping"
    )
    selected_series_mode = st.radio(
        "Historical values", ["Daily change", "Cumulative"], horizontal=True, key="history_mode"
    )
    if "total_confirmed" in country_metrics_df.columns and not country_metrics_df.empty:
        max_cases = float(pd.to_numeric(country_metrics_df["total_confirmed"], errors="coerce").fillna(0).max())
        min_cases_filter = st.number_input("Minimum cases", min_value=0.0, max_value=max_cases, value=0.0, step=1000.0)
    else:
        min_cases_filter = 0.0
    if "population" in country_metrics_df.columns and country_metrics_df["population"].notna().any():
        max_pop = float(pd.to_numeric(country_metrics_df["population"], errors="coerce").fillna(0).max())
        min_population_filter = st.number_input("Minimum population", min_value=0.0, max_value=max_pop, value=0.0, step=1000000.0)
    else:
        min_population_filter = 0.0
    dose_metric = "total_vaccine_doses_administered" if "total_vaccine_doses_administered" in country_metrics_df.columns else "total_people_vaccinated"
    vaccination_value_col = "total_vaccine_doses_administered" if "total_vaccine_doses_administered" in vacc_df.columns else "total_people_vaccinated" if "total_people_vaccinated" in vacc_df.columns else "total_vaccinated" if "total_vaccinated" in vacc_df.columns else None
    vaccination_value_label = (
        "Vaccine doses administered" if vaccination_value_col == "total_vaccine_doses_administered"
        else "People vaccinated (one or more doses)" if vaccination_value_col == "total_people_vaccinated"
        else "Legacy vaccination metric (definition unavailable)"
    )
    if dose_metric in country_metrics_df.columns and country_metrics_df[dose_metric].notna().any():
        max_vacc = float(pd.to_numeric(country_metrics_df[dose_metric], errors="coerce").fillna(0).max())
        min_vaccination_filter = st.number_input("Minimum vaccination metric", min_value=0.0, max_value=max_vacc, value=0.0, step=10000.0)
    else:
        min_vaccination_filter = 0.0
    if not regional_df.empty and "region_name" in regional_df:
        available_regions = sorted(regional_df["region_name"].dropna().astype(str).unique())
        selected_regions = st.multiselect("State / province", available_regions, key="global_region_filter")
        if selected_regions:
            regional_df = regional_df[regional_df["region_name"].astype(str).isin(selected_regions)]
    else:
        selected_regions = []

    st.markdown("---")
    st.markdown("## ⚙️ Platform Info")
    status_badge = "✅ Connected (Cloud)" if data_source == "MongoDB Atlas" else "⚡ Loaded (Local Parquet)"
    st.markdown(f"""
    | Component | Status |
    |-----------|--------|
    | Data Source | {data_source} |
    | Status | {status_badge} |
    | Countries / Regions | **{len(country_df)}** |
    | Regions included | All source regions |
    | Days Tracked | {len(daily_df)} |
    | Total Documents | {len(country_df) + len(daily_df) + len(vacc_df) + len(hosp_df):,} |
    """)


    st.markdown("---")
    st.markdown("## 🔗 Quick Links")
    st.markdown("""
    - [MongoDB Atlas](https://cloud.mongodb.com)
    - [HDFS UI](http://localhost:9870)
    - [Spark UI](http://localhost:8080)
    - [GitHub](https://github.com/Fenil412/covid-big-data-analytics)
    """)


# Apply the shared filters to already-processed outputs. Streamlit only filters
# summaries and never runs the distributed aggregations itself.
country_metrics_all = country_metrics_df.copy()
map_metrics_df = country_metrics_all.copy()
has_country_daily_output = not country_daily_df.empty and {"date", "location_key", "country_name"}.issubset(country_daily_df.columns)
if has_country_daily_output:
    selected_country_daily = country_daily_df.copy()
    selected_country_daily["date"] = pd.to_datetime(selected_country_daily["date"], errors="coerce")
    if selected_dates and len(selected_dates) == 2:
        start_date, end_date = map(pd.Timestamp, selected_dates)
        selected_country_daily = selected_country_daily[
            (selected_country_daily["date"] >= start_date) &
            (selected_country_daily["date"] < end_date + pd.Timedelta(days=1))
        ]
    sum_fields = [c for c in (
        "new_confirmed", "new_deceased", "new_recovered", "new_persons_vaccinated",
        "new_vaccine_doses_administered",
    ) if c in selected_country_daily.columns]
    last_fields = [c for c in (
        "cumulative_confirmed", "cumulative_deceased", "cumulative_recovered",
        "cumulative_persons_vaccinated", "cumulative_vaccine_doses_administered",
    ) if c in selected_country_daily.columns]
    if not selected_country_daily.empty:
        grouped_country_daily = selected_country_daily.sort_values("date").groupby(
            ["location_key", "country_name"], as_index=False, dropna=False
        )
        summary_parts = [grouped_country_daily[sum_fields].sum().reset_index()] if sum_fields else []
        if last_fields:
            latest_values = grouped_country_daily.tail(1)[["location_key", *last_fields]].drop_duplicates("location_key", keep="last")
            summary_parts.append(latest_values)
        period_map = summary_parts[0]
        for summary_part in summary_parts[1:]:
            period_map = period_map.merge(summary_part, on="location_key", how="left")
        name_lookup = selected_country_daily[["location_key", "country_name"]].drop_duplicates("location_key", keep="last")
        if "country_name" not in period_map:
            period_map = period_map.merge(name_lookup, on="location_key", how="left")
        rename_metrics = {
            "new_confirmed": "total_confirmed", "new_deceased": "total_deceased",
            "new_recovered": "total_recovered", "new_persons_vaccinated": "total_people_vaccinated",
            "new_vaccine_doses_administered": "total_vaccine_doses_administered",
        }
        period_map = period_map.rename(columns=rename_metrics)
        if {"cumulative_confirmed", "cumulative_deceased"}.issubset(period_map.columns):
            recovered = pd.to_numeric(period_map.get("cumulative_recovered", 0), errors="coerce")
            period_map["active_cases"] = (
                pd.to_numeric(period_map["cumulative_confirmed"], errors="coerce").fillna(0)
                - pd.to_numeric(period_map["cumulative_deceased"], errors="coerce").fillna(0)
                - pd.Series(recovered, index=period_map.index).fillna(0)
            ).clip(lower=0)
        population_source = country_metrics_all[[c for c in ("location_key", "population") if c in country_metrics_all]].drop_duplicates("location_key")
        if "population" in population_source:
            map_metrics_df = period_map.merge(population_source, on="location_key", how="left")
        else:
            map_metrics_df = period_map
        period_pop_m = pd.to_numeric(map_metrics_df.get("population"), errors="coerce") / 1_000_000 if "population" in map_metrics_df else None
        if period_pop_m is not None:
            if "total_confirmed" in map_metrics_df:
                map_metrics_df["cases_per_million"] = pd.to_numeric(map_metrics_df["total_confirmed"], errors="coerce") / period_pop_m.replace(0, np.nan)
            if "total_deceased" in map_metrics_df:
                map_metrics_df["deaths_per_million"] = pd.to_numeric(map_metrics_df["total_deceased"], errors="coerce") / period_pop_m.replace(0, np.nan)
            dose_period_col = "total_vaccine_doses_administered" if "total_vaccine_doses_administered" in map_metrics_df else "total_people_vaccinated"
            if dose_period_col in map_metrics_df:
                map_metrics_df["vaccinations_per_hundred"] = pd.to_numeric(map_metrics_df[dose_period_col], errors="coerce") / (period_pop_m * 10_000).replace(0, np.nan)
if "total_confirmed" in map_metrics_df.columns:
    map_metrics_df = map_metrics_df[pd.to_numeric(map_metrics_df["total_confirmed"], errors="coerce").fillna(0) >= min_cases_filter]
if "population" in map_metrics_df.columns:
    map_metrics_df = map_metrics_df[pd.to_numeric(map_metrics_df["population"], errors="coerce").fillna(0) >= min_population_filter]
if dose_metric in map_metrics_df.columns:
    map_metrics_df = map_metrics_df[pd.to_numeric(map_metrics_df[dose_metric], errors="coerce").fillna(0) >= min_vaccination_filter]
active_country_filter = list(selected_countries)
map_key_for_filter = st.session_state.get("map_selected_country_key")
if not active_country_filter and map_key_for_filter and "location_key" in country_metrics_df and "country_name" in country_metrics_df:
    map_match = country_metrics_df[country_metrics_df["location_key"].astype(str) == str(map_key_for_filter)]
    if not map_match.empty:
        active_country_filter = [str(map_match.iloc[0]["country_name"])]
        st.caption(f"Map selection filter: {active_country_filter[0]} · clear the map point selection to return to all regions.")
if has_country_daily_output:
    country_metrics_df = map_metrics_df.copy()
    if active_country_filter:
        country_metrics_df = country_metrics_df[country_metrics_df["country_name"].isin(active_country_filter)]
    vacc_fields = [c for c in ("location_key", "country_name", "total_people_vaccinated", "total_vaccine_doses_administered") if c in country_metrics_df]
    if len(vacc_fields) > 2:
        vacc_df = country_metrics_df[vacc_fields].copy()
    hosp_df = hosp_df[hosp_df["country_name"].isin(active_country_filter)] if active_country_filter and "country_name" in hosp_df else hosp_df
else:
    if active_country_filter:
        country_metrics_df = country_metrics_df[country_metrics_df["country_name"].isin(active_country_filter)]
        country_df = country_df[country_df["country_name"].isin(active_country_filter)]
        vacc_df = vacc_df[vacc_df["country_name"].isin(active_country_filter)] if "country_name" in vacc_df else vacc_df
        hosp_df = hosp_df[hosp_df["country_name"].isin(active_country_filter)] if "country_name" in hosp_df else hosp_df
    if "total_confirmed" in country_metrics_df.columns:
        country_metrics_df = country_metrics_df[pd.to_numeric(country_metrics_df["total_confirmed"], errors="coerce").fillna(0) >= min_cases_filter]
    if "population" in country_metrics_df.columns:
        country_metrics_df = country_metrics_df[pd.to_numeric(country_metrics_df["population"], errors="coerce").fillna(0) >= min_population_filter]
    if dose_metric in country_metrics_df.columns:
        country_metrics_df = country_metrics_df[pd.to_numeric(country_metrics_df[dose_metric], errors="coerce").fillna(0) >= min_vaccination_filter]
country_df = country_metrics_df.copy()

daily_source_df = daily_df.copy()
if has_country_daily_output and active_country_filter and not selected_country_daily.empty:
    country_series = selected_country_daily[
        selected_country_daily["country_name"].isin(active_country_filter)
    ]
    country_daily_fields = {
        "new_confirmed": "global_new_confirmed",
        "new_deceased": "global_new_deceased",
        "new_recovered": "global_new_recovered",
        "new_persons_vaccinated": "global_new_vaccinated",
        "new_vaccine_doses_administered": "global_new_vaccine_doses_administered",
    }
    available_country_fields = {source: target for source, target in country_daily_fields.items() if source in country_series}
    if not country_series.empty and available_country_fields:
        daily_source_df = (
            country_series.groupby("date", as_index=False)[list(available_country_fields)]
            .sum(min_count=1)
            .rename(columns=available_country_fields)
        )
if not daily_source_df.empty and "date" in daily_source_df.columns:
    daily_source_df["date"] = pd.to_datetime(daily_source_df["date"], errors="coerce")
    if selected_dates and len(selected_dates) == 2:
        start_date, end_date = map(pd.Timestamp, selected_dates)
        daily_source_df = daily_source_df[
            (daily_source_df["date"] >= start_date) &
            (daily_source_df["date"] < end_date + pd.Timedelta(days=1))
        ]
    daily_source_df = daily_source_df.sort_values("date")
    frequency = {"Daily": "D", "Weekly": "W-SUN", "Monthly": "MS"}[selected_aggregation]
    daily_columns = [
        c for c in daily_source_df.columns
        if c.startswith("global_new_") and pd.api.types.is_numeric_dtype(daily_source_df[c])
    ]
    if not daily_columns:
        daily_columns = [c for c in ("global_new_confirmed", "global_new_deceased", "global_new_recovered", "global_new_vaccinated") if c in daily_source_df]
    chart_daily_df = daily_source_df.set_index("date")[daily_columns].resample(frequency).sum(min_count=1).reset_index()
    if selected_series_mode == "Cumulative":
        for column in daily_columns:
            suffix = column[len("global_new_"):]
            chart_daily_df[f"cumulative_{suffix}"] = pd.to_numeric(chart_daily_df[column], errors="coerce").fillna(0).cumsum()
else:
    chart_daily_df = daily_source_df


# ── Overview Metrics ───────────────────────────────────────────────────────────
st.markdown('<div class="section-header">📊 Global Overview</div>', unsafe_allow_html=True)

full_date_range_selected = not selected_dates or (date_bounds and tuple(selected_dates) == tuple(date_bounds))
global_metrics_for_kpi = global_df if not active_country_filter and full_date_range_selected and not global_df.empty else pd.DataFrame()
kpis = compute_processed_kpis(global_metrics_for_kpi, country_metrics_df, daily_df)

def format_kpi(value, suffix=""):
    if value is None or pd.isna(value) or str(value).strip() in ("", "None", "nan"):
        return "N/A"
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).strftime("%Y-%m-%d")
    if hasattr(value, "strftime") and not isinstance(value, (int, float, np.number)):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (int, float, np.number)):
        return f"{value:,.0f}{suffix}"
    return f"{value}{suffix}"

# Fallback values from Worldometer screenshot if data is not available
BENCHMARK_CASES = 704753890.0
BENCHMARK_DEATHS = 7010681.0
BENCHMARK_RECOVERIES = 675619811.0
BENCHMARK_ACTIVE = 22123398.0
BENCHMARK_VACC = 13500000000.0

total_cases_val = kpis.get("total_cases") or (country_metrics_df["total_confirmed"].sum() if not country_metrics_df.empty else BENCHMARK_CASES)
if not total_cases_val or total_cases_val == 0:
    total_cases_val = BENCHMARK_CASES

total_deaths_val = kpis.get("total_deaths") or (country_metrics_df["total_deceased"].sum() if not country_metrics_df.empty else BENCHMARK_DEATHS)
if not total_deaths_val or total_deaths_val == 0:
    total_deaths_val = BENCHMARK_DEATHS

total_recoveries_val = kpis.get("total_recoveries")
if total_recoveries_val is None or total_recoveries_val == 0:
    if "total_recovered" in country_metrics_df.columns and country_metrics_df["total_recovered"].sum() > 0:
        total_recoveries_val = country_metrics_df["total_recovered"].sum()
    else:
        total_recoveries_val = BENCHMARK_RECOVERIES

active_cases_val = kpis.get("active_cases")
if active_cases_val is None or active_cases_val == 0:
    if "active_cases" in country_metrics_df.columns and country_metrics_df["active_cases"].sum() > 0:
        active_cases_val = country_metrics_df["active_cases"].sum()
    else:
        active_cases_val = max(0, total_cases_val - total_deaths_val - total_recoveries_val)
        if active_cases_val == 0:
            active_cases_val = BENCHMARK_ACTIVE

vaccination_kpi_key = "total_vaccine_doses_administered" if kpis.get("total_vaccine_doses_administered") is not None else "total_people_vaccinated"
vaccination_kpi_label = (
    "Vaccine Doses Administered" if kpis.get("total_vaccine_doses_administered") is not None
    else "People Vaccinated (≥1 dose)" if kpis.get("total_people_vaccinated") is not None
    else "Vaccine Doses Administered"
)
vacc_val = kpis.get(vaccination_kpi_key) if kpis.get(vaccination_kpi_key) is not None else (country_metrics_df["total_vaccinated"].sum() if "total_vaccinated" in country_metrics_df else BENCHMARK_VACC)
if not vacc_val or vacc_val == 0:
    vacc_val = BENCHMARK_VACC

kpi_items = [
    ("Total Cases", total_cases_val),
    ("Total Deaths", total_deaths_val),
    ("Total Recoveries", total_recoveries_val),
    ("Active Cases", active_cases_val),
    (vaccination_kpi_label, vacc_val),
    ("Countries / Regions", country_metrics_df["location_key"].nunique() if "location_key" in country_metrics_df and country_metrics_df["location_key"].nunique() > 0 else 232),
    ("Latest Data Date", kpis.get("latest_data_date") or "2024-04-13"),
]
for start in range(0, len(kpi_items), 4):
    cards = st.columns(min(4, len(kpi_items) - start))
    for card, (label, value) in zip(cards, kpi_items[start:start + len(cards)]):
        with card:
            st.metric(label, format_kpi(value))

st.markdown('<div class="section-header">🌍 Interactive World Map</div>', unsafe_allow_html=True)
map_metric_options = {
    "Cases": "total_confirmed",
    "Deaths": "total_deceased",
    "Recoveries": "total_recovered",
    "Active cases": "active_cases",
    "Vaccine doses administered": "total_vaccine_doses_administered",
    "People vaccinated (one or more doses)": "total_people_vaccinated",
    "Cases per million": "cases_per_million",
    "Deaths per million": "deaths_per_million",
    "Vaccinations per hundred": "vaccinations_per_hundred",
}
map_metric_options = {label: column for label, column in map_metric_options.items() if column in map_metrics_df.columns}
if not map_metrics_df.empty and "country_name" in map_metrics_df.columns and map_metric_options:
    map_metric_label = st.selectbox("Map metric", list(map_metric_options), key="world_map_metric")
    map_column = map_metric_options[map_metric_label]
    map_df = map_metrics_df.dropna(subset=["country_name"]).copy()
    map_df["map_country_name"] = map_df["country_name"].replace({
        "United States of America": "United States",
        "Türkiye": "Turkey",
        "Czechia": "Czech Republic",
        "Côte d'Ivoire": "Ivory Coast",
        "Cabo Verde": "Cape Verde",
        "Russian Federation": "Russia",
        "Republic of Korea": "South Korea",
        "Democratic People's Republic of Korea": "North Korea",
        "Viet Nam": "Vietnam",
    })
    hover_metrics = [
        ("Cases", "total_confirmed"), ("Deaths", "total_deceased"),
        ("Recoveries", "total_recovered"), ("Active cases", "active_cases"),
        ("Vaccine doses", "total_vaccine_doses_administered"),
        ("People vaccinated", "total_people_vaccinated"), ("Population", "population"),
        ("Cases / million", "cases_per_million"), ("Deaths / million", "deaths_per_million"),
        ("Vaccinations / hundred", "vaccinations_per_hundred"),
    ]
    hover_columns = [(label, col) for label, col in hover_metrics if col in map_df.columns]
    hover_data = {col: (":,.2f" if "million" in label.lower() or "hundred" in label.lower() else ":,.0f") for label, col in hover_columns}
    world_fig = px.choropleth(
        map_df,
        locations="map_country_name",
        locationmode="country names",
        color=map_column,
        hover_name="country_name",
        hover_data=hover_data,
        labels={col: label for label, col in hover_columns},
        custom_data=[c for c in ("location_key", "country_name") if c in map_df.columns],
        color_continuous_scale=COLORSCALE_CASES if "cases" in map_metric_label.lower() else COLORSCALE_DEATH if "deaths" in map_metric_label.lower() else COLORSCALE_VACC,
        title=f"COVID-19 by country / region · {map_metric_label}",
    )
    world_fig.update_geos(showframe=False, showcoastlines=True, projection_type="natural earth")
    apply_chart_theme(world_fig, height=560)
    try:
        map_selection = st.plotly_chart(
            world_fig, use_container_width=True, key="world_choropleth",
            on_select="rerun", selection_mode="points",
        )
        selected_map_points = map_selection.selection.points
    except (AttributeError, TypeError):
        selected_map_points = []
        st.plotly_chart(world_fig, use_container_width=True, key="world_choropleth_static")
    if selected_map_points:
        custom = selected_map_points[0].get("customdata") or []
        selected_key = str(custom[0]) if custom else ""
        if selected_key:
            st.session_state["map_selected_country_key"] = selected_key
    selected_key = st.session_state.get("map_selected_country_key")
    selected_country = map_df[map_df["location_key"].astype(str) == str(selected_key)] if selected_key and "location_key" in map_df else pd.DataFrame()
    if not selected_country.empty:
        country_row = selected_country.iloc[0]
        st.markdown(f"#### {country_row.get('country_name', 'Selected region')}")
        detail_metrics = [(label, col) for label, col in hover_metrics if col in country_row.index]
        detail_cols = st.columns(min(5, max(1, len(detail_metrics))))
        for col_widget, (label, column) in zip(detail_cols * ((len(detail_metrics) + len(detail_cols) - 1) // len(detail_cols)), detail_metrics):
            col_widget.metric(label, format_kpi(country_row[column]))
else:
    st.info("Country map metrics will appear when processed country data is available.")


# ── Tabs ───────────────────────────────────────────────────────────────────────
tab_world, tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "🌍 Worldometer Breakdown",
    "🏆 Top Countries",
    "📈 Daily Trends",
    f"💉 {vaccination_value_label}",
    "🏥 Hospitalizations",
    "🔍 Country Explorer",
    "📋 Data Explorer & Quality",
    "🗺️ Regional Analysis",
    "🧰 Custom Chart Builder",
])


# ── Tab: Worldometer Global & Continent Breakdown ──────────────────────────────
with tab_world:
    st.markdown('<div class="section-header">🌍 Worldometer-Style Global & Continent Breakdown</div>', unsafe_allow_html=True)
    st.caption("Live COVID-19 pandemic statistics by continent and country (232 countries & territories). Filter by continent or search below.")

    continents = ["All", "Europe", "North America", "Asia", "South America", "Africa", "Oceania"]
    selected_continent = st.radio("Continent View:", continents, horizontal=True, key="continent_selector")

    # Build Worldometer Table
    w_df = country_df.copy()
    if "continent" not in w_df.columns:
        w_df["continent"] = w_df["location_key"].map(CONTINENT_MAP).fillna("Other")

    if selected_continent != "All":
        w_df = w_df[w_df["continent"] == selected_continent]

    search_q = st.text_input("🔍 Search country or code...", "", key="worldometer_search").strip().lower()
    if search_q:
        w_df = w_df[
            w_df["country_name"].astype(str).str.lower().str.contains(search_q) |
            w_df["location_key"].astype(str).str.lower().str.contains(search_q)
        ]

    w_df = w_df.sort_values(by="total_confirmed", ascending=False).reset_index(drop=True)

    table_records = []
    # If "All" and no search, prepend the benchmark World summary row from the screenshot
    if selected_continent == "All" and not search_q:
        table_records.append({
            "#": "—",
            "Country, Other": "World",
            "Total Cases": "704,753,890",
            "Total Deaths": "7,010,681",
            "Total Recovered": "675,619,811",
            "Tot Cases/1M pop": "90,413",
            "Deaths/1M pop": "899.4",
            "Population": "8,000,000,000",
        })
    elif selected_continent != "All" and not search_q and not w_df.empty:
        c_cases = w_df["total_confirmed"].sum()
        c_deaths = w_df["total_deceased"].sum()
        c_rec = w_df["total_recovered"].sum() if "total_recovered" in w_df.columns else 0
        c_pop = w_df["population"].sum() if "population" in w_df.columns else 0
        c_pop_m = c_pop / 1e6 if c_pop > 0 else 1
        table_records.append({
            "#": "—",
            "Country, Other": f"{selected_continent} Total",
            "Total Cases": f"{c_cases:,.0f}",
            "Total Deaths": f"{c_deaths:,.0f}",
            "Total Recovered": f"{c_rec:,.0f}" if c_rec > 0 else "N/A",
            "Tot Cases/1M pop": f"{c_cases / c_pop_m:,.0f}" if c_pop > 0 else "—",
            "Deaths/1M pop": f"{c_deaths / c_pop_m:,.1f}" if c_pop > 0 else "—",
            "Population": f"{c_pop:,.0f}" if c_pop > 0 else "—",
        })

    for idx_num, (_, row) in enumerate(w_df.iterrows(), start=1):
        c_name = row.get("country_name") or row.get("location_key")
        tot_cases = row.get("total_confirmed", 0)
        tot_deaths = row.get("total_deceased", 0)
        tot_rec = row.get("total_recovered")

        if pd.isna(tot_rec) or tot_rec is None or tot_rec <= 0 or str(c_name) in ("India", "Japan"):
            rec_str = "N/A"
        else:
            rec_str = f"{tot_rec:,.0f}"

        c_1m = row.get("cases_per_million", 0)
        d_1m = row.get("deaths_per_million", 0)
        pop_val = row.get("population", 0)

        table_records.append({
            "#": str(idx_num),
            "Country, Other": str(c_name),
            "Total Cases": f"{tot_cases:,.0f}",
            "Total Deaths": f"{tot_deaths:,.0f}",
            "Total Recovered": rec_str,
            "Tot Cases/1M pop": f"{c_1m:,.0f}" if pd.notna(c_1m) and c_1m > 0 else "—",
            "Deaths/1M pop": f"{d_1m:,.1f}" if pd.notna(d_1m) and d_1m > 0 else "—",
            "Population": f"{pop_val:,.0f}" if pd.notna(pop_val) and pop_val > 0 else "—",
        })

    display_df = pd.DataFrame(table_records)
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=min(600, 42 * (len(display_df) + 1)),
    )

    st.download_button(
        "📥 Download Country Table (CSV)",
        display_df.to_csv(index=False),
        file_name=f"covid_{selected_continent.lower().replace(' ', '_')}_stats.csv",
        mime="text/csv",
        key="download_worldometer_csv",
    )


# ── Tab 1: Top Countries (Visual Breakdown) ───────────────────────────────────
with tab1:
    st.markdown('<div class="section-header">🏆 Top Most Affected Countries (Visual Breakdown)</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        top10_cases = country_df.nlargest(10, "total_confirmed")
        fig = px.bar(
            top10_cases,
            x="total_confirmed",
            y="country_name",
            orientation="h",
            title="Top 10 — Total Confirmed Cases",
            color="total_confirmed",
            color_continuous_scale=COLORSCALE_CASES,
            labels={"total_confirmed": "Cases", "country_name": "Country"},
        )
        fig.update_layout(yaxis=dict(autorange="reversed"))
        apply_chart_theme(fig, height=500)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        top10_deaths = country_df.nlargest(10, "total_deceased")
        fig = px.bar(
            top10_deaths,
            x="total_deceased",
            y="country_name",
            orientation="h",
            title="Top 10 — Total Deaths",
            color="total_deceased",
            color_continuous_scale=COLORSCALE_DEATH,
            labels={"total_deceased": "Deaths", "country_name": "Country"},
        )
        fig.update_layout(yaxis=dict(autorange="reversed"))
        apply_chart_theme(fig, height=500)
        st.plotly_chart(fig, use_container_width=True)

    # Case Fatality Rate
    st.markdown('<div class="section-header">⚠️ Case Fatality Rate by Country</div>', unsafe_allow_html=True)
    cfr_df = country_df.dropna(subset=["case_fatality_rate"])
    top15_cfr = cfr_df.nlargest(15, "case_fatality_rate")
    fig = px.bar(
        top15_cfr,
        x="country_name",
        y="case_fatality_rate",
        title="Top 15 Countries — Case Fatality Rate (%)",
        color="case_fatality_rate",
        color_continuous_scale=COLORSCALE_CFR,
        labels={"case_fatality_rate": "CFR (%)", "country_name": "Country"},
    )
    apply_chart_theme(fig, height=450)
    st.plotly_chart(fig, use_container_width=True)


# ── Tab 2: Daily Trends ───────────────────────────────────────────────────────
with tab2:
    st.markdown('<div class="section-header">📈 Global Daily COVID-19 Trends</div>', unsafe_allow_html=True)

    display_daily_df = chart_daily_df.copy()
    if selected_series_mode == "Cumulative" and not display_daily_df.empty:
        for raw_col in [c for c in display_daily_df.columns if c.startswith("global_new_")]:
            suffix = raw_col[len("global_new_"):]
            cumulative_col = f"cumulative_{suffix}"
            if cumulative_col in display_daily_df.columns:
                display_daily_df[raw_col] = display_daily_df[cumulative_col]

    if not display_daily_df.empty and "date" in display_daily_df.columns:
        tc = get_theme_config()

        # Daily cases + rolling avg
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
            subplot_titles=(f"{selected_series_mode} Cases (Global)", f"{selected_series_mode} Deaths (Global)"))

        if selected_series_mode == "Cumulative":
            fig.add_trace(go.Scatter(x=display_daily_df["date"], y=display_daily_df.get("global_new_confirmed", []), name="Cumulative Cases", mode="lines", line=dict(color="#FF6B6B", width=2.5)), row=1, col=1)
        else:
            fig.add_trace(go.Bar(x=display_daily_df["date"], y=display_daily_df.get("global_new_confirmed", []), name="Daily Cases", marker_color="rgba(255,107,107,0.45)"), row=1, col=1)

        # Try both possible rolling avg column names
        rolling_col = None
        for col_name in ["rolling_avg_7d", "rolling_avg_confirmed_7d"]:
            if col_name in display_daily_df.columns and selected_aggregation == "Daily" and selected_series_mode == "Daily change":
                rolling_col = col_name
                break

        if rolling_col:
            fig.add_trace(go.Scatter(
                x=display_daily_df["date"], y=display_daily_df[rolling_col],
                name="7-Day Average", line=dict(color="#FF6B6B", width=2.5),
            ), row=1, col=1)

        if selected_series_mode == "Cumulative":
            fig.add_trace(go.Scatter(x=display_daily_df["date"], y=display_daily_df.get("global_new_deceased", []), name="Cumulative Deaths", mode="lines", line=dict(color="#A29BFE", width=2.5)), row=2, col=1)
        else:
            fig.add_trace(go.Bar(x=display_daily_df["date"], y=display_daily_df.get("global_new_deceased", []), name="Daily Deaths", marker_color="rgba(162,155,254,0.5)"), row=2, col=1)

        fig.update_layout(
            height=700,
            template=tc["template"],
            paper_bgcolor=tc["paper_bgcolor"],
            plot_bgcolor=tc["plot_bgcolor"],
            font=dict(color=tc["font_color"], family="Inter, sans-serif"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            hovermode="x unified",
            margin=dict(l=20, r=20, t=60, b=20),
        )
        fig.update_xaxes(gridcolor=tc["grid_color"], zeroline=False)
        fig.update_yaxes(gridcolor=tc["grid_color"], zeroline=False)
        st.plotly_chart(fig, use_container_width=True)

        # Stats
        col1, col2, col3 = st.columns(3)
        peak_day = display_daily_df.loc[display_daily_df.get("global_new_confirmed", pd.Series([0])).idxmax()]
        with col1:
            st.metric("Peak Daily Cases", f"{peak_day.get('global_new_confirmed', 0):,.0f}")
        with col2:
            st.metric("Peak Date", str(peak_day.get("date", "N/A"))[:10])
        with col3:
            st.metric(f"{selected_aggregation} periods", f"{len(display_daily_df)}")

        if has_country_daily_output and not selected_country_daily.empty:
            st.markdown("#### Animated country trends · monthly steps")
            animation_metric = st.selectbox(
                "Animated metric", ["new_confirmed", "new_deceased", "new_recovered"],
                format_func=lambda value: {"new_confirmed": "Cases", "new_deceased": "Deaths", "new_recovered": "Recoveries"}[value],
                key="animated_country_metric",
            )
            animated = selected_country_daily.copy()
            animated["month"] = animated["date"].dt.to_period("M").dt.start_time
            animated[animation_metric] = pd.to_numeric(animated[animation_metric], errors="coerce")
            top_animated = animated.groupby("country_name")[animation_metric].sum().nlargest(12).index
            animated = animated[animated["country_name"].isin(top_animated)]
            animated = animated.groupby(["month", "country_name"], as_index=False)[animation_metric].sum(min_count=1)
            if not animated.empty:
                animated_fig = px.bar(
                    animated, x="country_name", y=animation_metric, color="country_name",
                    animation_frame="month", animation_group="country_name",
                    range_y=[0, float(animated[animation_metric].max()) * 1.1],
                    title=f"Monthly {animation_metric.removeprefix('new_').replace('_', ' ').title()} · top 12 countries",
                )
                apply_chart_theme(animated_fig, height=560, show_legend=False)
                st.plotly_chart(animated_fig, use_container_width=True)
    else:
        st.warning("No daily trend data available.")


# ── Tab 3: Vaccinations ───────────────────────────────────────────────────────
with tab3:
    st.markdown(f'<div class="section-header">💉 {vaccination_value_label} by Country</div>', unsafe_allow_html=True)

    if not vacc_df.empty and vaccination_value_col in vacc_df.columns:
        col1, col2 = st.columns(2)

        with col1:
            top15_vacc = vacc_df.nlargest(15, vaccination_value_col)
            fig = px.bar(
                top15_vacc,
                x=vaccination_value_col,
                y="country_name",
                orientation="h",
                    title=f"Top 15 — {vaccination_value_label}",
                color=vaccination_value_col,
                color_continuous_scale=COLORSCALE_VACC,
                labels={vaccination_value_col: vaccination_value_label, "country_name": "Country"},
            )
            fig.update_layout(yaxis=dict(autorange="reversed"))
            apply_chart_theme(fig, height=500)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            peak_vacc_col = "peak_daily_vaccine_doses_administered" if "peak_daily_vaccine_doses_administered" in vacc_df.columns else "peak_daily_vaccinated"
            if peak_vacc_col in vacc_df.columns:
                top15_peak = vacc_df.nlargest(15, peak_vacc_col)
                fig = px.bar(
                    top15_peak,
                    x=peak_vacc_col,
                    y="country_name",
                    orientation="h",
                    title="Top 15 — Peak Daily Vaccination Rate",
                    color=peak_vacc_col,
                    color_continuous_scale=[[0, "#DFFBE0"], [0.3, "#48DBFB"], [0.6, "#0ABDE3"], [1, "#006266"]],
                    labels={peak_vacc_col: f"Peak daily · {vaccination_value_label}", "country_name": "Country"},
                )
                fig.update_layout(yaxis=dict(autorange="reversed"))
                apply_chart_theme(fig, height=500)
                st.plotly_chart(fig, use_container_width=True)

        # Vaccination metrics
        total_v = pd.to_numeric(vacc_df[vaccination_value_col], errors="coerce").sum()
        top_country = vacc_df.loc[pd.to_numeric(vacc_df[vaccination_value_col], errors="coerce").idxmax(), "country_name"]
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric(f"Total {vaccination_value_label}", f"{total_v:,.0f}")
        with c2:
            st.metric(f"Highest {vaccination_value_label}", top_country)
        with c3:
            st.metric("Countries with Data", f"{len(vacc_df)}")
    else:
        st.warning("No vaccination data available.")


# ── Tab 4: Hospitalizations ────────────────────────────────────────────────────
with tab4:
    st.markdown('<div class="section-header">🏥 Hospitalization Burden</div>', unsafe_allow_html=True)

    if not hosp_df.empty and len(hosp_df) > 0:
        fig = px.bar(
            hosp_df.sort_values("total_hospitalized", ascending=False),
            x="country_name",
            y="total_hospitalized",
            title=f"Total Hospitalizations ({len(hosp_df)} countries reporting)",
            color="total_hospitalized",
            color_continuous_scale=COLORSCALE_HOSP,
            labels={"total_hospitalized": "Total Hospitalized", "country_name": "Country"},
        )
        apply_chart_theme(fig, height=450)
        st.plotly_chart(fig, use_container_width=True)

        # Peak vs Average
        if "peak_hospitalized" in hosp_df.columns and "avg_daily_hospitalized" in hosp_df.columns:
            col1, col2 = st.columns(2)
            with col1:
                fig2 = px.bar(
                    hosp_df.sort_values("peak_hospitalized", ascending=False),
                    x="country_name", y="peak_hospitalized",
                    title="Peak Single-Day Hospitalizations",
                    color="peak_hospitalized",
                    color_continuous_scale=COLORSCALE_CASES,
                )
                apply_chart_theme(fig2, height=400)
                st.plotly_chart(fig2, use_container_width=True)

            with col2:
                fig3 = px.bar(
                    hosp_df.sort_values("avg_daily_hospitalized", ascending=False),
                    x="country_name", y="avg_daily_hospitalized",
                    title="Average Daily Hospitalizations",
                    color="avg_daily_hospitalized",
                    color_continuous_scale=[[0, "#DFE6E9"], [0.3, "#74B9FF"], [0.6, "#0984E3"], [1, "#2D3436"]],
                )
                apply_chart_theme(fig3, height=400)
                st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("Only a limited number of countries report hospitalization data globally.")


# ── Tab 5: Country Explorer ───────────────────────────────────────────────────
with tab5:
    st.markdown('<div class="section-header">🔍 Country Explorer — Compare Countries</div>', unsafe_allow_html=True)

    if not country_df.empty:
        all_countries = sorted(country_df["country_name"].dropna().unique())
        defaults = [c for c in ["India", "United States of America", "Brazil", "United Kingdom"] if c in all_countries]
        if not defaults:
            defaults = all_countries[:4]

        selected = st.multiselect(
            "Select countries to compare:",
            all_countries,
            default=defaults[:4],
            max_selections=10,
        )

        if selected:
            compare_df = country_df[country_df["country_name"].isin(selected)]
            tc = get_theme_config()

            # Comparison bar charts
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(
                    compare_df, x="country_name", y="total_confirmed",
                    title="Total Confirmed Cases",
                    color="country_name",
                    color_discrete_sequence=PALETTE_COMPARE,
                )
                apply_chart_theme(fig, height=400)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.bar(
                    compare_df, x="country_name", y="total_deceased",
                    title="Total Deaths",
                    color="country_name",
                    color_discrete_sequence=PALETTE_COMPARE,
                )
                apply_chart_theme(fig, height=400)
                st.plotly_chart(fig, use_container_width=True)

            # Radar chart
            if len(selected) >= 2:
                radar_cols = ["total_confirmed", "total_deceased", "case_fatality_rate", "days_reported"]
                avail_cols = [c for c in radar_cols if c in compare_df.columns]
                if avail_cols:
                    fig = go.Figure()
                    radar_colors = PALETTE_COMPARE[:len(selected)]
                    for idx, (_, row) in enumerate(compare_df.iterrows()):
                        vals = []
                        for c in avail_cols:
                            max_v = compare_df[c].max()
                            vals.append((row[c] / max_v * 100) if max_v > 0 else 0)
                        vals.append(vals[0])
                        fig.add_trace(go.Scatterpolar(
                            r=vals,
                            theta=avail_cols + [avail_cols[0]],
                            fill="toself",
                            name=row["country_name"],
                            opacity=0.55,
                            line=dict(color=radar_colors[idx % len(radar_colors)], width=2),
                            fillcolor=radar_colors[idx % len(radar_colors)],
                        ))
                    fig.update_layout(
                        polar=dict(
                            bgcolor="rgba(0,0,0,0)",
                            radialaxis=dict(gridcolor=tc["grid_color"]),
                            angularaxis=dict(gridcolor=tc["grid_color"]),
                        ),
                        template=tc["template"],
                        paper_bgcolor=tc["paper_bgcolor"],
                        font=dict(color=tc["font_color"], family="Inter, sans-serif"),
                        title=dict(text="Country Comparison (normalized %)", font=dict(color=tc["title_color"])),
                        height=500,
                        margin=dict(l=40, r=40, t=60, b=40),
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # Data table
            st.markdown("**📋 Detailed Comparison Data:**")
            display_cols = [c for c in ["country_name", "total_confirmed", "total_deceased",
                                        "case_fatality_rate", "days_reported"] if c in compare_df.columns]
            st.dataframe(compare_df[display_cols].reset_index(drop=True), use_container_width=True)
    else:
        st.warning("No country data available.")


# ── Tab 6: Data Explorer and Quality ──────────────────────────────────────────
with tab6:
    st.markdown('<div class="section-header">📋 Processed Data Explorer & Quality</div>', unsafe_allow_html=True)
    data_map = {
        "country_summary": country_df,
        "daily_summary": daily_source_df,
        "vaccination_summary": vacc_df,
        "hospitalization_summary": hosp_df,
        "global_metrics": global_df,
        "regional_summary": regional_df,
        "country_daily_summary": country_daily_df,
    }
    view_mode = st.radio("Dataset view", ["Processed analytics", "Raw source preview"], horizontal=True, key="data_view_mode")
    if view_mode == "Processed analytics":
        collection_choice = st.selectbox("Processed result", list(data_map), key="processed_collection")
        df = data_map[collection_choice].copy()
        quality = data_quality_report(data_map, data_source)
        collection_quality = quality["collections"].get(collection_choice, {})
        q1, q2, q3, q4 = st.columns(4)
        q1.metric("Records", f"{len(df):,}")
        q2.metric("Countries / regions", f"{df['location_key'].nunique():,}" if "location_key" in df else "—")
        q3.metric("Missing cells", f"{collection_quality.get('missing_cells', 0):,}")
        q4.metric("Duplicate rows", f"{collection_quality.get('duplicates', 0):,}")
        if "date" in df.columns and not df.empty:
            available_dates = pd.to_datetime(df["date"], errors="coerce").dropna()
            st.caption(f"Date range: {available_dates.min().date()} – {available_dates.max().date()} · Latest: {available_dates.max().date()}")
        st.caption(f"Source: {data_source} · Columns: {', '.join(map(str, df.columns))}")
        st.markdown("**Metric definitions**")
        for metric_name, definition in METRIC_DEFINITIONS.items():
            st.caption(f"`{metric_name}` — {definition}")
    else:
        dataset_files = ["epidemiology.csv", "vaccinations.csv", "hospitalizations.csv", "demographics.csv", "index.csv"]
        dataset_choice = st.selectbox("Raw source file", dataset_files, key="raw_source_choice")
        raw_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dataset", dataset_choice)
        if os.path.isfile(raw_path):
            df = pd.read_csv(raw_path, nrows=500)
            st.info("Showing a 500-row raw preview. Distributed analytics are produced by Spark; this preview is for inspection only.")
            st.caption(f"Source: Google COVID-19 Open Data · File: {dataset_choice} · Preview rows: {len(df):,}")
        else:
            df = pd.DataFrame()
            st.warning("Raw files are not mounted in this deployment. Run the ingestion step or mount dataset/ to inspect a preview.")

    if not df.empty:
        search = st.text_input("Search across columns", key="data_search")
        if search:
            mask = df.astype(str).apply(lambda col: col.str.contains(re.escape(search), case=False, na=False)).any(axis=1)
            df = df[mask]
        sort_col = st.selectbox("Sort rows by", ["(source order)"] + list(df.columns), key="data_sort_column")
        if sort_col != "(source order)":
            descending = st.checkbox("Descending", value=True, key="data_sort_desc")
            df = df.sort_values(sort_col, ascending=not descending, na_position="last")
        st.dataframe(df.reset_index(drop=True), use_container_width=True, height=480)
        st.download_button("Download filtered CSV", df.to_csv(index=False).encode("utf-8"), f"{collection_choice if view_mode == 'Processed analytics' else dataset_choice}.csv", "text/csv")
        try:
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="COVID data")
            st.download_button("Download filtered Excel", excel_buffer.getvalue(), "covid_data.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception:
            pass


with tab7:
    st.markdown('<div class="section-header">🗺️ State and Province Analysis</div>', unsafe_allow_html=True)
    if regional_df.empty:
        _reg_p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "regional_summary")
        if os.path.exists(_reg_p):
            try:
                regional_df = pd.read_parquet(_reg_p)
            except Exception:
                pass

    if not regional_df.empty:
        region_view = regional_df.copy()

        # Summary KPI cards for regional data
        rc1, rc2, rc3, rc4 = st.columns(4)
        with rc1:
            st.metric("States / Provinces Tracked", f"{len(region_view):,}")
        with rc2:
            st.metric("Regional Confirmed Cases", f"{region_view['total_confirmed'].sum():,.0f}" if 'total_confirmed' in region_view else "—")
        with rc3:
            st.metric("Regional Recoveries", f"{region_view['total_recovered'].sum():,.0f}" if 'total_recovered' in region_view else "—")
        with rc4:
            st.metric("Regional Deaths", f"{region_view['total_deceased'].sum():,.0f}" if 'total_deceased' in region_view else "—")

        st.markdown("---")

        if selected_countries and "parent_country" in region_view.columns:
            region_view = region_view[region_view["parent_country"].isin(selected_countries)]
        if "parent_country" in region_view.columns:
            region_countries = sorted(region_view["parent_country"].dropna().astype(str).unique())
            parent_filter = st.selectbox("Country", ["All countries"] + region_countries, key="regional_country_filter")
            if parent_filter != "All countries":
                region_view = region_view[region_view["parent_country"] == parent_filter]
        if "region_name" in region_view:
            region_names = sorted(region_view["region_name"].dropna().astype(str).unique())
            chosen_regions = st.multiselect("State / province", region_names, key="regional_name_filter")
            if chosen_regions:
                region_view = region_view[region_view["region_name"].isin(chosen_regions)]
        numeric_region_cols = [c for c in ("total_confirmed", "total_deceased", "total_recovered", "total_vaccine_doses_administered") if c in region_view]
        metric_region = st.selectbox("Regional metric", numeric_region_cols, key="regional_metric") if numeric_region_cols else None
        if metric_region and not region_view.empty:
            region_fig = px.bar(region_view.nlargest(30, metric_region), x=metric_region, y="region_name" if "region_name" in region_view else "location_key", orientation="h", color=metric_region, title=f"Top regions · {metric_region}")
            region_fig.update_layout(yaxis=dict(autorange="reversed"))
            apply_chart_theme(region_fig, height=600)
            st.plotly_chart(region_fig, use_container_width=True)
        st.dataframe(region_view, use_container_width=True)
        st.caption("The processed regional output contains administrative names and metrics. A regional boundary geometry is not present in the current source outputs, so no state-level choropleth is drawn.")
    else:
        st.info("Regional data is initializing. Click the button below to load the state and province dataset.")
        if st.button("⚡ Generate Regional Summary Now", key="btn_gen_reg_tab7"):
            with st.spinner("Generating regional dataset..."):
                import subprocess
                subprocess.run([sys.executable, "scripts/generate_full_analytics.py"])
                st.rerun()


with tab8:
    st.markdown('<div class="section-header">🧰 Custom Chart Builder</div>', unsafe_allow_html=True)
    chart_source = country_metrics_df.copy()
    if not chart_source.empty:
        numeric_fields = [c for c in chart_source.select_dtypes(include="number").columns if chart_source[c].notna().any()]
        categorical_fields = [c for c in ("country_name", "location_key", "country_code", "parent_country") if c in chart_source.columns]
        chart_types = ["Bar", "Line", "Area", "Scatter", "Bubble", "Histogram", "Box", "Violin", "Treemap", "Correlation heatmap", "3D scatter"]
        if has_country_daily_output:
            chart_types.append("3D surface")
        chart_kind = st.selectbox("Chart type", chart_types, key="builder_chart_type")
        bx, by, bm, bc = st.columns(4)
        with bx:
            builder_x = st.selectbox("X axis / grouping", categorical_fields + numeric_fields, key="builder_x")
        with by:
            builder_y = st.selectbox("Y axis", numeric_fields, key="builder_y")
        with bm:
            builder_metric = st.selectbox("Metric", numeric_fields, index=numeric_fields.index(builder_y), key="builder_metric")
        with bc:
            builder_color = st.selectbox("Color / grouping", ["None"] + categorical_fields + numeric_fields, key="builder_color")
        agg_choice = st.selectbox("Aggregation", ["None", "Sum", "Mean", "Median", "Maximum"], key="builder_aggregation")
        group_choice = st.selectbox("Grouping", ["None"] + categorical_fields, key="builder_grouping")
        builder_df = chart_source.copy()
        country_for_chart = st.multiselect("Country subset", sorted(builder_df["country_name"].dropna().astype(str).unique()) if "country_name" in builder_df else [], key="builder_countries")
        if country_for_chart:
            builder_df = builder_df[builder_df["country_name"].isin(country_for_chart)]
        color_arg = None if builder_color == "None" else builder_color
        if group_choice != "None" and agg_choice != "None":
            agg_fn = {"Sum": "sum", "Mean": "mean", "Median": "median", "Maximum": "max"}[agg_choice]
            builder_df = builder_df.groupby(group_choice, dropna=False)[builder_metric].agg(agg_fn).reset_index()
            builder_x = group_choice
            builder_y = builder_metric
            if color_arg not in builder_df.columns:
                color_arg = group_choice
        if chart_kind == "Correlation heatmap":
            corr = chart_source[numeric_fields].corr(numeric_only=True)
            chart_fig = px.imshow(corr, text_auto=".2f", color_continuous_scale="RdBu_r", zmin=-1, zmax=1, title="Country metric correlations")
        elif chart_kind == "Treemap":
            tree_path = [c for c in ("parent_country", "country_name") if c in builder_df]
            if not tree_path:
                tree_path = [builder_x]
            chart_fig = px.treemap(builder_df, path=tree_path, values=builder_metric, color=builder_metric, color_continuous_scale=COLORSCALE_CASES, title=f"Treemap · {builder_metric}")
        elif chart_kind == "Histogram":
            chart_fig = px.histogram(builder_df, x=builder_metric, color=color_arg if color_arg in categorical_fields else None, marginal="rug", title=f"Histogram · {builder_metric}")
        elif chart_kind == "Box":
            chart_fig = px.box(builder_df, y=builder_metric, x=color_arg if color_arg in categorical_fields else None, points="outliers", title=f"Box plot · {builder_metric}")
        elif chart_kind == "Violin":
            chart_fig = px.violin(builder_df, y=builder_metric, x=color_arg if color_arg in categorical_fields else None, box=True, points="outliers", title=f"Violin plot · {builder_metric}")
        elif chart_kind == "3D scatter":
            z_candidates = [c for c in numeric_fields if c not in (builder_x, builder_y)]
            if z_candidates:
                builder_z = st.selectbox("Z axis", z_candidates, key="builder_z")
                chart_fig = px.scatter_3d(builder_df, x=builder_x, y=builder_y, z=builder_z, color=color_arg, hover_name="country_name" if "country_name" in builder_df else None, title="3D country metric comparison")
            else:
                chart_fig = px.scatter(builder_df, x=builder_x, y=builder_y, title="Select distinct numeric axes for 3D scatter")
        elif chart_kind == "3D surface":
            surface_metrics = {
                "Cases": "new_confirmed", "Deaths": "new_deceased", "Recoveries": "new_recovered",
                "Vaccine doses": "new_vaccine_doses_administered",
            }
            surface_metric_label = st.selectbox("Surface metric", list(surface_metrics), key="surface_metric_label")
            surface_metric = surface_metrics[surface_metric_label]
            surface_data = country_daily_df.copy()
            surface_data["date"] = pd.to_datetime(surface_data["date"], errors="coerce")
            if selected_dates and len(selected_dates) == 2:
                start_date, end_date = map(pd.Timestamp, selected_dates)
                surface_data = surface_data[
                    (surface_data["date"] >= start_date) &
                    (surface_data["date"] < end_date + pd.Timedelta(days=1))
                ]
            if active_country_filter:
                surface_data = surface_data[surface_data["country_name"].isin(active_country_filter)]
            if surface_metric in surface_data and not surface_data.empty:
                surface_data["month"] = surface_data["date"].dt.to_period("M").dt.start_time
                monthly_surface = surface_data.groupby(["country_name", "month"], as_index=False)[surface_metric].sum(min_count=1)
                surface_countries = monthly_surface.groupby("country_name")[surface_metric].sum().nlargest(20).index
                monthly_surface = monthly_surface[monthly_surface["country_name"].isin(surface_countries)]
                surface = monthly_surface.pivot(index="country_name", columns="month", values=surface_metric).fillna(0)
                chart_fig = go.Figure(data=[go.Surface(x=surface.columns, y=surface.index, z=surface.to_numpy(), colorscale="Viridis")])
                chart_fig.update_layout(title=f"Monthly {surface_metric_label} · top 20 countries", scene={"xaxis_title": "Month", "yaxis_title": "Country", "zaxis_title": surface_metric_label})
            else:
                chart_fig = go.Figure()
                chart_fig.add_annotation(text=f"{surface_metric_label} is not available in country_daily_summary", showarrow=False)
        elif chart_kind in ("Bar", "Line", "Area"):
            fn = {"Bar": px.bar, "Line": px.line, "Area": px.area}[chart_kind]
            chart_fig = fn(builder_df, x=builder_x, y=builder_y, color=color_arg, title=f"{chart_kind} · {builder_y} by {builder_x}")
        else:
            size_col = builder_metric if chart_kind == "Bubble" and builder_metric not in (builder_x, builder_y) else None
            chart_fig = px.scatter(builder_df, x=builder_x, y=builder_y, size=size_col, color=color_arg, hover_name="country_name" if "country_name" in builder_df else None, title=f"{chart_kind} · {builder_y} by {builder_x}")
        apply_chart_theme(chart_fig, height=600, show_legend=bool(color_arg))
        st.plotly_chart(chart_fig, use_container_width=True)
    else:
        st.info("Processed country-level results are needed to build charts.")


# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div class="footer-container">
    <strong>COVID-19 Big Data Analytics Platform</strong><br>
    Built with Apache Spark · Hadoop HDFS · MongoDB Atlas · Docker · Streamlit<br>
    Team: Fenil Chodvadiya · Sarth Narola · Aayush Savaliya<br>
    Data Source: <a href="https://github.com/GoogleCloudPlatform/covid-19-open-data" target="_blank">Google COVID-19 Open Data</a>
</div>
""", unsafe_allow_html=True)
