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

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

# ── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="COVID-19 Big Data Analytics",
    page_icon="🦠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    .main-header {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
        padding: 2rem 2.5rem;
        border-radius: 16px;
        margin-bottom: 2rem;
        color: white;
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
    }
    .main-header h1 {
        margin: 0;
        font-size: 2rem;
        font-weight: 700;
        letter-spacing: -0.5px;
    }
    .main-header p {
        margin: 0.5rem 0 0 0;
        opacity: 0.8;
        font-size: 1rem;
    }

    .metric-card {
        background: linear-gradient(135deg, #1e1e2f, #2a2a40);
        border-radius: 14px;
        padding: 1.5rem;
        text-align: center;
        border: 1px solid rgba(255,255,255,0.08);
        box-shadow: 0 4px 20px rgba(0,0,0,0.2);
        transition: transform 0.2s, box-shadow 0.2s;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 30px rgba(0,0,0,0.3);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #e94560;
        margin: 0.5rem 0;
    }
    .metric-label {
        font-size: 0.85rem;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .section-header {
        font-size: 1.4rem;
        font-weight: 600;
        color: #e94560;
        margin: 2rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid rgba(233,69,96,0.3);
    }

    .tech-badge {
        display: inline-block;
        background: rgba(233,69,96,0.15);
        color: #e94560;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 6px;
        margin-bottom: 4px;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
    }

    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)


# ── MongoDB Connection ─────────────────────────────────────────────────────────
@st.cache_resource
def get_mongo_client():
    """Connect to MongoDB Atlas."""
    from pymongo import MongoClient
    uri = os.getenv("MONGODB_ATLAS_URI", "")
    if not uri:
        st.error("❌ MONGODB_ATLAS_URI not set in .env file!")
        st.stop()
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client


@st.cache_data(ttl=300)
def load_collection(collection_name: str) -> pd.DataFrame:
    """Load a MongoDB collection into a Pandas DataFrame."""
    client = get_mongo_client()
    db = client[os.getenv("MONGODB_DATABASE", "covid_analytics")]
    docs = list(db[collection_name].find({}, {"_id": 0}))
    if not docs:
        return pd.DataFrame()
    return pd.DataFrame(docs)


# ── Load Data ──────────────────────────────────────────────────────────────────
try:
    country_df = load_collection("country_summary")
    daily_df = load_collection("daily_summary")
    vacc_df = load_collection("vaccination_summary")
    hosp_df = load_collection("hospitalization_summary")
except Exception as e:
    st.error(f"❌ Failed to connect to MongoDB Atlas: {e}")
    st.info("Make sure Docker is running or run `python run_pipeline_local.py` first.")
    st.stop()

# Convert date column
if "date" in daily_df.columns:
    daily_df["date"] = pd.to_datetime(daily_df["date"])
    daily_df = daily_df.sort_values("date")


# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <h1>🦠 COVID-19 Big Data Analytics Dashboard</h1>
    <p>Distributed analytics on 12.5M+ records using Hadoop HDFS + Apache Spark + MongoDB Atlas</p>
    <div style="margin-top: 12px;">
        <span class="tech-badge">Apache Spark</span>
        <span class="tech-badge">Hadoop HDFS</span>
        <span class="tech-badge">MongoDB Atlas</span>
        <span class="tech-badge">Docker</span>
        <span class="tech-badge">PySpark</span>
        <span class="tech-badge">Python</span>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Platform Info")
    st.markdown(f"""
    | Component | Status |
    |-----------|--------|
    | MongoDB Atlas | ✅ Connected |
    | Countries | {len(country_df)} |
    | Days Tracked | {len(daily_df)} |
    | Total Docs | {len(country_df) + len(daily_df) + len(vacc_df) + len(hosp_df):,} |
    """)

    st.markdown("---")
    st.markdown("## 👥 Team")
    st.markdown("""
    - **Fenil** — Lead Engineer (50%)
    - **Sarth** — Cluster & Ingestion (25%)
    - **Aayush** — Analytics & NoSQL (25%)
    """)

    st.markdown("---")
    st.markdown("## 🔗 Quick Links")
    st.markdown("""
    - [MongoDB Atlas](https://cloud.mongodb.com)
    - [HDFS UI](http://localhost:9870)
    - [Spark UI](http://localhost:8080)
    - [GitHub](https://github.com/Fenil412/covid-big-data-analytics)
    """)


# ── Overview Metrics ───────────────────────────────────────────────────────────
st.markdown('<div class="section-header">📊 Global Overview</div>', unsafe_allow_html=True)

total_cases = country_df["total_confirmed"].sum() if "total_confirmed" in country_df.columns else 0
total_deaths = country_df["total_deceased"].sum() if "total_deceased" in country_df.columns else 0
total_vacc = vacc_df["total_vaccinated"].sum() if "total_vaccinated" in vacc_df.columns else 0
avg_cfr = country_df["case_fatality_rate"].mean() if "case_fatality_rate" in country_df.columns else 0

c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.metric("🌍 Countries", f"{len(country_df)}")
with c2:
    st.metric("🦠 Total Cases", f"{total_cases / 1e9:.2f}B")
with c3:
    st.metric("💀 Total Deaths", f"{total_deaths / 1e6:.1f}M")
with c4:
    st.metric("💉 Vaccinations", f"{total_vacc / 1e9:.2f}B")
with c5:
    st.metric("📈 Avg CFR", f"{avg_cfr:.2f}%")


# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏆 Top Countries",
    "📈 Daily Trends",
    "💉 Vaccinations",
    "🏥 Hospitalizations",
    "🔍 Country Explorer",
    "📋 Raw Data",
])


# ── Tab 1: Top Countries ──────────────────────────────────────────────────────
with tab1:
    st.markdown('<div class="section-header">🏆 Top 10 Most Affected Countries</div>', unsafe_allow_html=True)

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
            color_continuous_scale="Reds",
            labels={"total_confirmed": "Cases", "country_name": "Country"},
        )
        fig.update_layout(
            yaxis=dict(autorange="reversed"),
            showlegend=False,
            coloraxis_showscale=False,
            height=500,
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
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
            color_continuous_scale="Purples",
            labels={"total_deceased": "Deaths", "country_name": "Country"},
        )
        fig.update_layout(
            yaxis=dict(autorange="reversed"),
            showlegend=False,
            coloraxis_showscale=False,
            height=500,
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
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
        color_continuous_scale="YlOrRd",
        labels={"case_fatality_rate": "CFR (%)", "country_name": "Country"},
    )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=450,
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig, use_container_width=True)


# ── Tab 2: Daily Trends ───────────────────────────────────────────────────────
with tab2:
    st.markdown('<div class="section-header">📈 Global Daily COVID-19 Trends</div>', unsafe_allow_html=True)

    if not daily_df.empty and "date" in daily_df.columns:
        # Daily cases + rolling avg
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                            subplot_titles=("Daily New Cases (Global)", "Daily Deaths (Global)"))

        fig.add_trace(go.Bar(
            x=daily_df["date"], y=daily_df.get("global_new_confirmed", []),
            name="Daily Cases", marker_color="rgba(233,69,96,0.4)",
        ), row=1, col=1)

        if "rolling_avg_7d" in daily_df.columns:
            fig.add_trace(go.Scatter(
                x=daily_df["date"], y=daily_df["rolling_avg_7d"],
                name="7-Day Average", line=dict(color="#e94560", width=2.5),
            ), row=1, col=1)

        fig.add_trace(go.Bar(
            x=daily_df["date"], y=daily_df.get("global_new_deceased", []),
            name="Daily Deaths", marker_color="rgba(106,76,147,0.5)",
        ), row=2, col=1)

        fig.update_layout(
            height=700,
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Stats
        col1, col2, col3 = st.columns(3)
        peak_day = daily_df.loc[daily_df.get("global_new_confirmed", pd.Series([0])).idxmax()]
        with col1:
            st.metric("Peak Daily Cases", f"{peak_day.get('global_new_confirmed', 0):,.0f}")
        with col2:
            st.metric("Peak Date", str(peak_day.get("date", "N/A"))[:10])
        with col3:
            st.metric("Days Tracked", f"{len(daily_df)}")
    else:
        st.warning("No daily trend data available.")


# ── Tab 3: Vaccinations ───────────────────────────────────────────────────────
with tab3:
    st.markdown('<div class="section-header">💉 Vaccination Progress by Country</div>', unsafe_allow_html=True)

    if not vacc_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            top15_vacc = vacc_df.nlargest(15, "total_vaccinated")
            fig = px.bar(
                top15_vacc,
                x="total_vaccinated",
                y="country_name",
                orientation="h",
                title="Top 15 — Total Vaccinations",
                color="total_vaccinated",
                color_continuous_scale="Greens",
                labels={"total_vaccinated": "Vaccinations", "country_name": "Country"},
            )
            fig.update_layout(
                yaxis=dict(autorange="reversed"),
                showlegend=False,
                coloraxis_showscale=False,
                height=500,
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            if "peak_daily_vaccinated" in vacc_df.columns:
                top15_peak = vacc_df.nlargest(15, "peak_daily_vaccinated")
                fig = px.bar(
                    top15_peak,
                    x="peak_daily_vaccinated",
                    y="country_name",
                    orientation="h",
                    title="Top 15 — Peak Daily Vaccination Rate",
                    color="peak_daily_vaccinated",
                    color_continuous_scale="Teal",
                    labels={"peak_daily_vaccinated": "Peak Daily", "country_name": "Country"},
                )
                fig.update_layout(
                    yaxis=dict(autorange="reversed"),
                    showlegend=False,
                    coloraxis_showscale=False,
                    height=500,
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)

        # Vaccination metrics
        total_v = vacc_df["total_vaccinated"].sum()
        top_country = vacc_df.loc[vacc_df["total_vaccinated"].idxmax(), "country_name"]
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Total Vaccinations", f"{total_v / 1e9:.2f}B")
        with c2:
            st.metric("Most Vaccinated Country", top_country)
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
            color_continuous_scale="OrRd",
            labels={"total_hospitalized": "Total Hospitalized", "country_name": "Country"},
        )
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=450,
            coloraxis_showscale=False,
        )
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
                    color_continuous_scale="Reds",
                )
                fig2.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                                   plot_bgcolor="rgba(0,0,0,0)", height=400, coloraxis_showscale=False)
                st.plotly_chart(fig2, use_container_width=True)

            with col2:
                fig3 = px.bar(
                    hosp_df.sort_values("avg_daily_hospitalized", ascending=False),
                    x="country_name", y="avg_daily_hospitalized",
                    title="Average Daily Hospitalizations",
                    color="avg_daily_hospitalized",
                    color_continuous_scale="Blues",
                )
                fig3.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                                   plot_bgcolor="rgba(0,0,0,0)", height=400, coloraxis_showscale=False)
                st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("Only 15 countries report hospitalization data globally.")


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

            # Comparison bar charts
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(
                    compare_df, x="country_name", y="total_confirmed",
                    title="Total Confirmed Cases",
                    color="country_name",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                                   plot_bgcolor="rgba(0,0,0,0)", showlegend=False, height=400)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.bar(
                    compare_df, x="country_name", y="total_deceased",
                    title="Total Deaths",
                    color="country_name",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                                   plot_bgcolor="rgba(0,0,0,0)", showlegend=False, height=400)
                st.plotly_chart(fig, use_container_width=True)

            # Radar chart
            if len(selected) >= 2:
                radar_cols = ["total_confirmed", "total_deceased", "case_fatality_rate", "days_reported"]
                avail_cols = [c for c in radar_cols if c in compare_df.columns]
                if avail_cols:
                    fig = go.Figure()
                    for _, row in compare_df.iterrows():
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
                            opacity=0.6,
                        ))
                    fig.update_layout(
                        polar=dict(bgcolor="rgba(0,0,0,0)"),
                        template="plotly_dark",
                        paper_bgcolor="rgba(0,0,0,0)",
                        title="Country Comparison (normalized %)",
                        height=500,
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # Data table
            st.markdown("**Detailed Data:**")
            display_cols = [c for c in ["country_name", "total_confirmed", "total_deceased",
                                        "case_fatality_rate", "days_reported"] if c in compare_df.columns]
            st.dataframe(compare_df[display_cols].reset_index(drop=True), use_container_width=True)
    else:
        st.warning("No country data available.")


# ── Tab 6: Raw Data ───────────────────────────────────────────────────────────
with tab6:
    st.markdown('<div class="section-header">📋 Raw Data Explorer</div>', unsafe_allow_html=True)

    collection_choice = st.selectbox(
        "Select collection:",
        ["country_summary", "daily_summary", "vaccination_summary", "hospitalization_summary"]
    )

    data_map = {
        "country_summary": country_df,
        "daily_summary": daily_df,
        "vaccination_summary": vacc_df,
        "hospitalization_summary": hosp_df,
    }

    df = data_map[collection_choice]
    st.markdown(f"**{collection_choice}** — {len(df)} documents")

    # Search filter
    if "country_name" in df.columns:
        search = st.text_input("🔍 Search by country name:", "")
        if search:
            df = df[df["country_name"].str.contains(search, case=False, na=False)]

    st.dataframe(df.reset_index(drop=True), use_container_width=True, height=500)

    # Download button
    csv = df.to_csv(index=False)
    st.download_button(
        label=f"📥 Download {collection_choice}.csv",
        data=csv,
        file_name=f"{collection_choice}.csv",
        mime="text/csv",
    )


# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; font-size: 0.85rem; padding: 1rem 0;">
    <strong>COVID-19 Big Data Analytics Platform</strong><br>
    Built with Apache Spark · Hadoop HDFS · MongoDB Atlas · Docker · Streamlit<br>
    Team: Fenil Chodvadiya · Sarth Narola · Aayush Savaliya<br>
    Data Source: <a href="https://github.com/GoogleCloudPlatform/covid-19-open-data" target="_blank">Google COVID-19 Open Data</a>
</div>
""", unsafe_allow_html=True)
