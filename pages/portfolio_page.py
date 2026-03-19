"""
pages/portfolio_page.py — Portfolio tracker.

Shows the user's holdings (from the 'portfolio' table), enriched with the
latest close price from the 'prices' table to display current value, gain/loss,
and weight.

Adding/removing positions and AI-powered commentary (via Claude API) will be
built out in a future session. This page lays the structural groundwork.
"""

import streamlit as st
import pandas as pd
from database import get_connection


def _load_portfolio() -> pd.DataFrame:
    """Join portfolio holdings with latest prices."""
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            p.id,
            p.ticker,
            s.name,
            s.market,
            p.shares,
            p.avg_cost,
            p.currency,
            -- Latest close price for this ticker
            (SELECT close FROM prices WHERE ticker = p.ticker ORDER BY date DESC LIMIT 1) AS current_price,
            -- Latest price date
            (SELECT date  FROM prices WHERE ticker = p.ticker ORDER BY date DESC LIMIT 1) AS price_date
        FROM portfolio p
        JOIN stocks s ON s.ticker = p.ticker
        ORDER BY p.ticker
        """
    ).fetchall()
    conn.close()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        rows,
        columns=["id", "Ticker", "Name", "Market", "Shares", "Avg cost",
                 "Currency", "Current price", "Price date"],
    )

    # Derived columns
    df["Cost basis"]    = df["Shares"] * df["Avg cost"]
    df["Market value"]  = df["Shares"] * df["Current price"]
    df["Gain/Loss"]     = df["Market value"] - df["Cost basis"]
    df["Return %"]      = (df["Gain/Loss"] / df["Cost basis"] * 100).round(2)

    total_value = df["Market value"].sum()
    df["Weight %"] = (df["Market value"] / total_value * 100).round(2) if total_value else 0.0

    return df


def _add_position(ticker: str, shares: float, avg_cost: float, currency: str) -> None:
    conn = get_connection()
    with conn:
        conn.execute(
            "INSERT INTO portfolio (ticker, shares, avg_cost, currency) VALUES (?, ?, ?, ?)",
            (ticker, shares, avg_cost, currency),
        )
    conn.close()


def _delete_position(position_id: int) -> None:
    conn = get_connection()
    with conn:
        conn.execute("DELETE FROM portfolio WHERE id = ?", (position_id,))
    conn.close()


def render() -> None:
    """Entry point called by app.py."""
    st.header("Portfolio")

    df = _load_portfolio()

    if not df.empty:
        # Summary metrics row
        total_cost  = df["Cost basis"].sum()
        total_value = df["Market value"].sum()
        total_gl    = df["Gain/Loss"].sum()
        total_ret   = (total_gl / total_cost * 100) if total_cost else 0.0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total cost basis",  f"{total_cost:,.0f}")
        col2.metric("Total market value", f"{total_value:,.0f}")
        col3.metric("Total gain/loss",    f"{total_gl:+,.0f}")
        col4.metric("Total return",       f"{total_ret:+.2f}%")

        st.divider()

        # Holdings table (hide the internal id column)
        display_cols = ["Ticker", "Name", "Market", "Shares", "Avg cost",
                        "Current price", "Price date", "Market value",
                        "Gain/Loss", "Return %", "Weight %", "Currency"]
        st.dataframe(df[display_cols], use_container_width=True, hide_index=True)

        # Delete a position
        st.subheader("Remove position")
        id_to_delete = st.selectbox(
            "Select position ID to remove",
            options=df["id"].tolist(),
            format_func=lambda i: f"{df[df['id']==i]['Ticker'].values[0]} (id {i})",
        )
        if st.button("Remove position", type="secondary"):
            _delete_position(id_to_delete)
            st.rerun()

    else:
        st.info("No positions yet. Add your first position below.")

    # ── Add position form ────────────────────────────────────────────────────
    st.divider()
    st.subheader("Add position")

    # Populate ticker dropdown from stocks already in the DB
    conn = get_connection()
    available_tickers = [r[0] for r in conn.execute("SELECT ticker FROM stocks ORDER BY ticker").fetchall()]
    conn.close()

    if not available_tickers:
        st.warning("No stocks in database yet. Import data on the **Import** page first.")
        return

    with st.form("add_position_form"):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            ticker   = st.selectbox("Ticker", available_tickers)
        with col2:
            shares   = st.number_input("Shares", min_value=0.0001, step=1.0, format="%.4f")
        with col3:
            avg_cost = st.number_input("Avg cost / share", min_value=0.0, step=0.01, format="%.2f")
        with col4:
            currency = st.selectbox("Currency", ["SEK", "USD", "EUR", "NOK", "DKK", "GBP"])

        if st.form_submit_button("Add position", type="primary"):
            if shares <= 0 or avg_cost <= 0:
                st.error("Shares and average cost must be greater than zero.")
            else:
                _add_position(ticker, shares, avg_cost, currency)
                st.success(f"Added {shares} shares of {ticker} at {avg_cost} {currency}.")
                st.rerun()
