"""
pages/import_page.py — Börsdata CSV import page.

Workflow:
  1. User fills in stock metadata (ticker, name, market, sector, currency)
  2. User uploads a CSV exported from Börsdata
  3. Parser converts it to DataFrames
  4. Data is upserted into the SQLite 'stocks' and 'prices' tables
  5. A preview of the imported rows is shown

The page never touches the screener or portfolio tables — it's purely
responsible for raw data ingestion.
"""

import streamlit as st
import pandas as pd
from components.borsdata_parser import parse_borsdata_csv
from database import get_connection


def _upsert_stock(ticker: str, name: str, market: str, sector: str, currency: str) -> None:
    """Insert or replace the stock metadata row."""
    conn = get_connection()
    with conn:
        conn.execute(
            """
            INSERT INTO stocks (ticker, name, market, sector, currency, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(ticker) DO UPDATE SET
                name       = excluded.name,
                market     = excluded.market,
                sector     = excluded.sector,
                currency   = excluded.currency,
                updated_at = excluded.updated_at
            """,
            (ticker, name, market, sector, currency),
        )
    conn.close()


def _upsert_prices(prices_df: pd.DataFrame) -> int:
    """Bulk-insert price rows, replacing on conflict. Returns number of rows written."""
    conn = get_connection()
    rows_written = 0
    with conn:
        for _, row in prices_df.iterrows():
            conn.execute(
                """
                INSERT INTO prices (ticker, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker, date) DO UPDATE SET
                    open   = excluded.open,
                    high   = excluded.high,
                    low    = excluded.low,
                    close  = excluded.close,
                    volume = excluded.volume
                """,
                (
                    row["ticker"],
                    row["date"],
                    row.get("open"),
                    row.get("high"),
                    row.get("low"),
                    row["close"],
                    row.get("volume"),
                ),
            )
            rows_written += 1
    conn.close()
    return rows_written


def render() -> None:
    """Entry point called by app.py."""
    st.header("Import Börsdata CSV")
    st.markdown(
        "Export price history from [Börsdata](https://borsdata.se) and upload the CSV here. "
        "Existing rows for the same ticker+date are overwritten."
    )

    # ── Stock metadata form ──────────────────────────────────────────────────
    with st.form("stock_meta_form"):
        st.subheader("Stock metadata")
        col1, col2 = st.columns(2)
        with col1:
            ticker = st.text_input("Ticker *", placeholder="e.g. ERIC-B, AAPL, VOW3").strip().upper()
            name   = st.text_input("Company name *", placeholder="e.g. Ericsson, Apple")
            market = st.text_input("Market", placeholder="e.g. Stockholmsbörsen, Nasdaq, XETRA")
        with col2:
            sector   = st.text_input("Sector", placeholder="e.g. Technology, Industrials")
            currency = st.selectbox("Currency", ["SEK", "USD", "EUR", "NOK", "DKK", "GBP"])

        st.subheader("CSV file")
        uploaded_file = st.file_uploader(
            "Upload Börsdata CSV export",
            type=["csv", "txt"],
            help="Export from Börsdata: choose a stock → Historia → Export CSV",
        )

        submitted = st.form_submit_button("Import", type="primary")

    # ── Processing ───────────────────────────────────────────────────────────
    if submitted:
        # Validate required fields
        errors = []
        if not ticker:
            errors.append("Ticker is required.")
        if not name:
            errors.append("Company name is required.")
        if uploaded_file is None:
            errors.append("Please upload a CSV file.")

        if errors:
            for e in errors:
                st.error(e)
            return

        try:
            file_bytes = uploaded_file.read()
            _, prices_df = parse_borsdata_csv(file_bytes, ticker)

            # Persist to database
            _upsert_stock(ticker, name, market, sector, currency)
            rows_written = _upsert_prices(prices_df)

            st.success(f"Imported **{rows_written}** price rows for **{ticker}** ({name}).")

            # Preview the most recent 10 rows
            st.subheader("Preview — most recent rows")
            preview = prices_df.sort_values("date", ascending=False).head(10)
            st.dataframe(preview, use_container_width=True, hide_index=True)

        except ValueError as e:
            st.error(f"Parse error: {e}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")
            raise  # re-raise so the full traceback appears in the terminal

    # ── Existing stocks in DB ────────────────────────────────────────────────
    st.divider()
    st.subheader("Stocks already in database")
    try:
        conn = get_connection()
        rows = conn.execute(
            """
            SELECT s.ticker, s.name, s.market, s.sector, s.currency,
                   COUNT(p.date) AS price_rows,
                   MAX(p.date)   AS latest_date
            FROM stocks s
            LEFT JOIN prices p ON p.ticker = s.ticker
            GROUP BY s.ticker
            ORDER BY s.ticker
            """
        ).fetchall()
        conn.close()

        if rows:
            st.dataframe(
                pd.DataFrame(rows, columns=["Ticker", "Name", "Market", "Sector", "Currency", "Price rows", "Latest date"]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No stocks imported yet. Upload a CSV above to get started.")
    except Exception as e:
        st.warning(f"Could not load existing stocks: {e}")
