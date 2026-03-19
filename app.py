"""
app.py — PortfolioTrendMonitor entry point.

Run with:
    streamlit run app.py

Navigation is handled via st.sidebar radio buttons. Each page lives in its
own module under /pages and exposes a single render() function so this file
stays minimal and each page can be developed independently.
"""

import streamlit as st
from database import init_db
from pages import import_page, screener_page, portfolio_page

# ── Page config (must be the first Streamlit call) ────────────────────────────
st.set_page_config(
    page_title="PortfolioTrendMonitor",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Ensure all SQLite tables exist before any page tries to query them
init_db()

# ── Sidebar navigation ────────────────────────────────────────────────────────
PAGES = {
    "📥 Import": import_page,
    "🔍 Screener": screener_page,
    "💼 Portfolio": portfolio_page,
}

with st.sidebar:
    st.title("PortfolioTrendMonitor")
    st.caption("Nordic investor toolkit — US, EU & Swedish stocks")
    st.divider()

    selection = st.radio(
        "Navigate",
        list(PAGES.keys()),
        label_visibility="collapsed",
    )

# ── Render selected page ──────────────────────────────────────────────────────
PAGES[selection].render()
