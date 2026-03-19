"""
app.py — PortfolioTrendMonitor entry point.

Run with:
    streamlit run app.py
"""

import streamlit as st
from database import init_db
from views import import_page, screener_page, portfolio_page

st.set_page_config(
    page_title="PortfolioTrendMonitor",
    page_icon="📈",
    layout="wide",
)

init_db()

PAGES = {
    "📥 Import": import_page,
    "🔍 Screener": screener_page,
    "💼 Portfolio": portfolio_page,
}

with st.sidebar:
    st.title("PortfolioTrendMonitor")
    st.markdown("---")
    page_label = st.radio("Navigate", list(PAGES.keys()), label_visibility="collapsed")

PAGES[page_label].render()
