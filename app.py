"""
app.py — PortfolioTrendMonitor entry point.

Run with:
    streamlit run app.py
"""

import streamlit as st
from database import init_db
from pages import import_page

st.set_page_config(
    page_title="PortfolioTrendMonitor",
    page_icon="📈",
    layout="wide",
)

init_db()
import_page.render()
