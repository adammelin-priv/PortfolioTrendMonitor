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

# Initialise page in session state so selection survives reruns
if "current_page" not in st.session_state:
    st.session_state.current_page = "📥 Import"

with st.sidebar:
    st.title("PortfolioTrendMonitor")
    st.markdown("---")
    for label in PAGES:
        is_active = st.session_state.current_page == label
        if st.button(
            label,
            use_container_width=True,
            type="primary" if is_active else "secondary",
            key=f"nav_{label}",
        ):
            st.session_state.current_page = label
            st.rerun()

PAGES[st.session_state.current_page].render()
