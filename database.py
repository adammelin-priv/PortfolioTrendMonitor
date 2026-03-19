"""
database.py — SQLite schema setup and shared connection helper.

All tables are created here on first run (CREATE TABLE IF NOT EXISTS),
so importing this module is safe to call repeatedly.
"""

import sqlite3
from pathlib import Path

# Database lives in /data so it's separate from source code
DB_PATH = Path(__file__).parent / "data" / "portfolio.db"


def get_connection() -> sqlite3.Connection:
    """Return a connection to the SQLite database with row_factory set
    so rows can be accessed as dicts (row["column_name"])."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # better concurrent read performance
    return conn


def init_db() -> None:
    """Create all tables if they don't already exist.
    Call once at app startup."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    with conn:
        conn.executescript("""
            -- Core stock metadata: one row per ticker
            CREATE TABLE IF NOT EXISTS stocks (
                ticker      TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                market      TEXT,           -- e.g. "Nasdaq", "Stockholmsbörsen", "XETRA"
                sector      TEXT,           -- e.g. "Technology", "Industrials"
                currency    TEXT,           -- e.g. "USD", "SEK", "EUR"
                updated_at  TEXT DEFAULT (datetime('now'))
            );

            -- Daily OHLCV price data imported from Börsdata CSV exports
            CREATE TABLE IF NOT EXISTS prices (
                ticker      TEXT NOT NULL,
                date        TEXT NOT NULL,  -- ISO-8601 "YYYY-MM-DD"
                open        REAL,
                high        REAL,
                low         REAL,
                close       REAL NOT NULL,
                volume      INTEGER,
                PRIMARY KEY (ticker, date),
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
            );

            -- Computed momentum/trend signals produced by the screener
            CREATE TABLE IF NOT EXISTS signals (
                ticker              TEXT NOT NULL,
                date                TEXT NOT NULL,  -- date signal was computed for
                momentum_score      REAL,           -- e.g. rate-of-change over N periods
                trend_direction     TEXT,           -- "up", "down", "sideways"
                ma_50               REAL,           -- 50-day moving average
                ma_200              REAL,           -- 200-day moving average
                rsi_14              REAL,           -- 14-period RSI
                computed_at         TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (ticker, date),
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
            );

            -- Pre-computed screener data imported directly from Börsdata screener exports
            CREATE TABLE IF NOT EXISTS screener_imports (
                ticker              TEXT PRIMARY KEY,
                borsdata_id         INTEGER,
                company             TEXT,
                instrument          TEXT,           -- e.g. "Stocks"
                sector              TEXT,
                country             TEXT,
                market              TEXT,           -- exchange / list (e.g. "Nasdaq")
                industry            TEXT,
                pe_current          REAL,
                peg_current         REAL,
                price_ma200_pct     REAL,           -- Price / MA200 deviation %
                ma200_trend_1m      REAL,           -- 1-month trend vs MA200 %
                perf_3m             REAL,           -- 3-month performance %
                perf_6m             REAL,           -- 6-month performance %
                perf_3y             REAL,           -- 3-year performance %
                roe_avg_3y          REAL,
                roe_current         REAL,
                net_debt_ebitda     REAL,
                profit_margin       REAL,
                profit_margin_avg   REAL,
                gross_margin        REAL,
                gross_margin_avg    REAL,
                earnings_growth_5y  REAL,
                revenue_growth_5y   REAL,
                revenue_growth_yy   REAL,
                revenue_growth_1y   REAL,
                dividend_growth_5y  REAL,
                market_cap_sek      REAL,
                opcashflow_stable   INTEGER,
                earnings_stable     INTEGER,
                banks_credit_losses REAL,
                banks_ci_ratio      REAL,
                imported_at         TEXT DEFAULT (datetime('now'))
            );

            -- User-defined screening configurations / presets
            CREATE TABLE IF NOT EXISTS screen_configs (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                name                    TEXT NOT NULL UNIQUE,
                description             TEXT,
                -- Technical / computed signal criteria
                trend_direction         TEXT,   -- "up", "down", "sideways", or NULL (= any)
                min_momentum_score      REAL,
                max_momentum_score      REAL,
                min_rsi                 REAL,
                max_rsi                 REAL,
                require_above_ma50      INTEGER DEFAULT 0,   -- boolean 0/1
                require_above_ma200     INTEGER DEFAULT 0,
                require_golden_cross    INTEGER DEFAULT 0,   -- MA50 > MA200
                -- Fundamental / Börsdata criteria
                max_pe                  REAL,
                min_roe                 REAL,
                max_net_debt_ebitda     REAL,
                min_profit_margin       REAL,
                min_perf_3m             REAL,
                min_perf_6m             REAL,
                min_earnings_growth_5y  REAL,
                min_revenue_growth_5y   REAL,
                min_price_ma200_pct     REAL,
                max_price_ma200_pct     REAL,
                -- Universe filters
                country                 TEXT,
                market                  TEXT,
                sector                  TEXT,
                created_at              TEXT DEFAULT (datetime('now')),
                updated_at              TEXT DEFAULT (datetime('now'))
            );

            -- User's portfolio positions
            CREATE TABLE IF NOT EXISTS portfolio (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker          TEXT NOT NULL,
                shares          REAL NOT NULL,
                avg_cost        REAL NOT NULL,      -- average cost per share in local currency
                currency        TEXT NOT NULL,
                added_at        TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
            );
        """)
    # Migrate existing databases: add new columns if they don't exist yet
    _add_column_if_missing(conn, "screener_imports", "borsdata_id", "INTEGER")
    _add_column_if_missing(conn, "screener_imports", "instrument", "TEXT")
    _add_column_if_missing(conn, "screener_imports", "banks_credit_losses", "REAL")
    _add_column_if_missing(conn, "screener_imports", "banks_ci_ratio", "REAL")

    conn.close()


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, col_type: str) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        conn.commit()
