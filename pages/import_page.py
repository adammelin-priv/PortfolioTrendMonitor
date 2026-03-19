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

import pandas as pd
import streamlit as st

from components.borsdata_parser import parse_borsdata_csv, parse_borsdata_screener_csv
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


def _upsert_screener_imports(df: pd.DataFrame) -> int:
    """Insert or replace screener_imports rows. Returns count written."""
    screener_cols = [
        "ticker", "company", "sector", "country", "market", "industry",
        "pe_current", "peg_current", "price_ma200_pct", "ma200_trend_1m",
        "perf_3m", "perf_6m", "perf_3y", "roe_avg_3y", "roe_current",
        "net_debt_ebitda", "profit_margin", "profit_margin_avg",
        "gross_margin", "gross_margin_avg", "earnings_growth_5y",
        "revenue_growth_5y", "revenue_growth_yy", "revenue_growth_1y",
        "dividend_growth_5y", "market_cap_sek", "opcashflow_stable", "earnings_stable",
    ]

    def _clean(v):
        """Convert NaN / float NaN to None for SQLite."""
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        return v

    conn = get_connection()
    rows_written = 0
    placeholders = ", ".join(["?"] * len(screener_cols))
    cols_str = ", ".join(screener_cols)
    update_str = ", ".join(
        f"{c} = excluded.{c}" for c in screener_cols if c != "ticker"
    )
    sql = (
        f"INSERT INTO screener_imports ({cols_str}) VALUES ({placeholders}) "
        f"ON CONFLICT(ticker) DO UPDATE SET {update_str}, imported_at = datetime('now')"
    )

    with conn:
        for _, row in df.iterrows():
            values = [_clean(row.get(c)) for c in screener_cols]
            conn.execute(sql, values)
            rows_written += 1
    conn.close()
    return rows_written


def render() -> None:
    """Entry point called by app.py."""
    st.header("Import Börsdata CSV")

    tab_prices, tab_screener = st.tabs(["Price History", "Börsdata Screener"])

    # ── Tab 1: OHLCV price history ────────────────────────────────────────────
    with tab_prices:
        st.markdown(
            "Export price history for a single stock from Börsdata "
            "(**stock page → Historia → Export CSV**) and upload here. "
            "Existing rows for the same ticker+date are overwritten."
        )

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
                "Upload Börsdata price history CSV",
                type=["csv", "txt"],
                help="Export from Börsdata: choose a stock → Historia → Export CSV",
            )
            submitted = st.form_submit_button("Import", type="primary")

        if submitted:
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
            else:
                try:
                    file_bytes = uploaded_file.read()
                    _, prices_df = parse_borsdata_csv(file_bytes, ticker)
                    _upsert_stock(ticker, name, market, sector, currency)
                    rows_written = _upsert_prices(prices_df)
                    st.success(f"Imported **{rows_written}** price rows for **{ticker}** ({name}).")
                    st.subheader("Preview — most recent rows")
                    preview = prices_df.sort_values("date", ascending=False).head(10)
                    st.dataframe(preview, use_container_width=True, hide_index=True)
                except ValueError as e:
                    st.error(f"Parse error: {e}")
                except Exception as e:
                    st.error(f"Unexpected error: {e}")
                    raise

        st.divider()
        st.subheader("Stocks already in database")
        try:
            conn = get_connection()
            rows = conn.execute(
                """
                SELECT s.ticker, s.name, s.market, s.sector, s.currency,
                       COUNT(p.date) AS price_rows, MAX(p.date) AS latest_date
                FROM stocks s
                LEFT JOIN prices p ON p.ticker = s.ticker
                GROUP BY s.ticker
                ORDER BY s.ticker
                """
            ).fetchall()
            conn.close()
            if rows:
                st.dataframe(
                    pd.DataFrame(rows, columns=["Ticker", "Name", "Market", "Sector",
                                                "Currency", "Price rows", "Latest date"]),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No stocks imported yet.")
        except Exception as e:
            st.warning(f"Could not load existing stocks: {e}")

    # ── Tab 2: Börsdata screener export ───────────────────────────────────────
    with tab_screener:
        st.markdown(
            "Export the screener/ranking list from Börsdata "
            "(**Screener → select columns → Export**) and upload here. "
            "The file must contain an **'Info - Ticker'** column. "
            "One row per stock — existing tickers are overwritten."
        )

        with st.form("screener_import_form"):
            screener_file = st.file_uploader(
                "Upload Börsdata screener CSV",
                type=["csv", "txt"],
                help="Tab-delimited export from the Börsdata screener tool",
            )
            screener_submitted = st.form_submit_button("Import screener data", type="primary")

        if screener_submitted:
            if screener_file is None:
                st.error("Please upload a CSV file.")
            else:
                try:
                    df = parse_borsdata_screener_csv(screener_file.read())
                    rows_written = _upsert_screener_imports(df)
                    st.success(f"Imported **{rows_written}** stocks from screener export.")
                    preview_cols = [c for c in
                                    ["ticker", "company", "country", "market", "sector",
                                     "pe_current", "perf_3m", "perf_6m", "roe_avg_3y",
                                     "profit_margin"]
                                    if c in df.columns]
                    st.subheader("Preview")
                    st.dataframe(df[preview_cols].head(20), use_container_width=True, hide_index=True)
                except ValueError as e:
                    st.error(f"Parse error: {e}")
                except Exception as e:
                    st.error(f"Unexpected error: {e}")
                    raise

        st.divider()
        st.subheader("Screener data already in database")
        try:
            conn = get_connection()
            rows = conn.execute(
                "SELECT ticker, company, country, market, sector, imported_at "
                "FROM screener_imports ORDER BY ticker"
            ).fetchall()
            conn.close()
            if rows:
                st.dataframe(
                    pd.DataFrame(rows, columns=["Ticker", "Company", "Country",
                                                "Market", "Sector", "Imported at"]),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No screener data imported yet.")
        except Exception as e:
            st.warning(f"Could not load screener data: {e}")
