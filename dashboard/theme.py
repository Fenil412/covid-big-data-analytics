"""Full-page Light/Dark theme for Streamlit + Plotly."""

from __future__ import annotations

import streamlit as st


def generate_theme_css(mode: str) -> str:
    """Return CSS targeting Streamlit containers for Light or Dark mode."""
    dark = mode == "Dark"
    bg = "#0e1117" if dark else "#ffffff"
    sidebar = "#161b22" if dark else "#f8f9fb"
    text = "#fafafa" if dark else "#1a1a2e"
    muted = "#9ca3af" if dark else "#64748b"
    card = "#1c2128" if dark else "#f1f5f9"
    input_bg = "#21262d" if dark else "#ffffff"
    border = "rgba(255,255,255,0.08)" if dark else "rgba(0,0,0,0.08)"

    return f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

html, body, [class*="css"] {{
    font-family: 'Inter', sans-serif;
    color: {text};
}}

[data-testid="stAppViewContainer"] {{
    background-color: {bg} !important;
    color: {text} !important;
}}

[data-testid="stMain"], [data-testid="stMainBlockContainer"], .stApp {{
    background-color: {bg} !important;
    color: {text} !important;
}}

[data-testid="stHeader"] {{
    background-color: transparent;
}}

[data-testid="stSidebar"] {{
    background-color: {sidebar} !important;
    border-right: 1px solid {border};
}}

[data-testid="stSidebar"] > div:first-child {{
    background-color: {sidebar} !important;
}}

[data-testid="stSidebar"] * {{
    color: {text} !important;
}}

[data-testid="stMetric"] {{
    background: {card};
    padding: 0.75rem 1rem;
    border-radius: 12px;
    border: 1px solid {border};
}}

[data-testid="stMetricValue"] {{
    color: {text};
    font-size: 1.6rem;
    font-weight: 700;
}}

[data-testid="stMetricLabel"] {{
    color: {muted};
    font-weight: 600;
    font-size: 0.78rem;
    text-transform: uppercase;
}}

.stTabs [data-baseweb="tab-list"] {{
    gap: 6px;
    border-bottom: 2px solid {border};
}}

.stTabs [data-baseweb="tab"] {{
    color: {muted};
    font-weight: 600;
}}

.stTabs [aria-selected="true"] {{
    color: {text};
    border-bottom: 3px solid #A29BFE;
}}

div[data-baseweb="input"] input,
div[data-baseweb="select"] > div,
textarea {{
    background-color: {input_bg} !important;
    color: {text} !important;
}}

.stButton > button {{
    background-color: {card} !important;
    color: {text} !important;
    border-color: {border} !important;
    border-radius: 10px;
    font-weight: 600;
}}

.main-header {{
    background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
    padding: 2rem 2.2rem;
    border-radius: 18px;
    margin-bottom: 1.5rem;
    color: white;
    box-shadow: 0 10px 40px rgba(0,0,0,0.25);
}}

.main-header h1 {{
    margin: 0;
    font-size: 2rem;
    font-weight: 800;
}}

.main-header p {{
    margin: 0.5rem 0 0;
    opacity: 0.9;
    color: #d0d0e8;
}}

.section-header {{
    font-size: 1.25rem;
    font-weight: 700;
    margin: 1.5rem 0 0.75rem;
    padding-bottom: 0.5rem;
    border-bottom: 3px solid;
    border-image: linear-gradient(90deg, #FF6B6B, #A29BFE, #00D2D3) 1;
}}

.tech-badge {{
    display: inline-block;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 0.72rem;
    font-weight: 600;
    margin: 2px 4px 2px 0;
    background: rgba(162,155,254,0.15);
    border: 1px solid rgba(162,155,254,0.25);
}}

.footer-container {{
    text-align: center;
    font-size: 0.82rem;
    opacity: 0.75;
    padding: 1rem 0;
}}
</style>
"""


def get_theme_config() -> dict:
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
    return {
        "template": "plotly_white",
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "font_color": "#2c3e50",
        "grid_color": "rgba(0,0,0,0.08)",
        "title_color": "#1a1a2e",
    }


def apply_chart_theme(fig, height: int = 500, show_legend: bool = False):
    tc = get_theme_config()
    fig.update_layout(
        template=tc["template"],
        paper_bgcolor=tc["paper_bgcolor"],
        plot_bgcolor=tc["plot_bgcolor"],
        font=dict(color=tc["font_color"], family="Inter, sans-serif"),
        title_font=dict(color=tc["title_color"], size=16),
        height=height,
        showlegend=show_legend,
        margin=dict(l=20, r=20, t=50, b=20),
    )
    fig.update_xaxes(gridcolor=tc["grid_color"], zeroline=False)
    fig.update_yaxes(gridcolor=tc["grid_color"], zeroline=False)
    return fig


PLOTLY_CONFIG = {
    "scrollZoom": True,
    "displayModeBar": True,
    "toImageButtonOptions": {"format": "png", "height": 800, "width": 1200},
}

COLORSCALE_CASES = [[0, "#FFEAA7"], [0.3, "#FDCB6E"], [0.6, "#E17055"], [1, "#D63031"]]
COLORSCALE_DEATH = [[0, "#DFE6E9"], [0.3, "#A29BFE"], [0.6, "#6C5CE7"], [1, "#2D3436"]]
COLORSCALE_VACC = [[0, "#DFFBE0"], [0.3, "#55EFC4"], [0.6, "#00B894"], [1, "#006266"]]
PALETTE_COMPARE = ["#FF6B6B", "#48DBFB", "#FECA57", "#00D2D3", "#A29BFE", "#FF9FF3", "#54A0FF", "#5F27CD"]
