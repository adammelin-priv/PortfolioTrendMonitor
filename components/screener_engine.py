"""
components/screener_engine.py — Compute momentum/trend signals from price history.

Reads daily close prices from the 'prices' table, computes technical indicators
per ticker, and upserts the latest signal row into the 'signals' table.

Indicators computed:
  - RSI-14  (Wilder's smoothing / EMA)
  - MA-50   (simple 50-day moving average)
  - MA-200  (simple 200-day moving average)
  - Momentum score (weighted rate-of-change over 1m / 3m / 6m)
  - Trend direction ("up" | "down" | "sideways")
"""

import pandas as pd

from database import get_connection

# Trading-day approximations
_1M = 20
_3M = 63
_6M = 126


# ── Signal computation helpers ────────────────────────────────────────────────

def _rsi(series: pd.Series, period: int = 14) -> float:
    """Return the last RSI value using Wilder's smoothed EMA method."""
    if len(series) < period + 1:
        return float("nan")
    delta = series.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return float((100 - 100 / (1 + rs)).iloc[-1])


def _momentum_score(series: pd.Series) -> float:
    """
    Weighted average rate-of-change: 1m (weight 0.2), 3m (0.4), 6m (0.4).
    Returns NaN when there is insufficient history for all three windows.
    Uses whatever windows are available and re-weights accordingly.
    """
    close = float(series.iloc[-1])

    def _roc(lookback: int) -> float:
        if len(series) < lookback + 1:
            return float("nan")
        past = float(series.iloc[-(lookback + 1)])
        return (close - past) / past * 100 if past != 0 else float("nan")

    candidates = [(_roc(_1M), 0.2), (_roc(_3M), 0.4), (_roc(_6M), 0.4)]
    valid = [(v, w) for v, w in candidates if not pd.isna(v)]
    if not valid:
        return float("nan")
    total_w = sum(w for _, w in valid)
    return round(sum(v * w for v, w in valid) / total_w, 4)


def _trend_direction(close: float, ma50: float | None, ma200: float | None) -> str:
    """
    Classify as 'up', 'down', or 'sideways'.
    'up'   : price > MA50 and MA50 > MA200  (both required when available)
    'down' : price < MA50 and MA50 < MA200
    Anything else → 'sideways'
    """
    if ma50 is None and ma200 is None:
        return "sideways"
    if ma200 is None:
        return "up" if close > ma50 else "down"
    if ma50 is None:
        return "up" if close > ma200 else "down"
    if close > ma50 and ma50 > ma200:
        return "up"
    if close < ma50 and ma50 < ma200:
        return "down"
    return "sideways"


# ── Public API ────────────────────────────────────────────────────────────────

def run_screener() -> dict:
    """
    Compute signals for every ticker that has price data and write the latest
    signal row to the 'signals' table (upsert).

    Returns a summary dict:
        {"tickers_processed": int, "tickers_skipped": int}
    """
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT ticker, date, close FROM prices ORDER BY ticker, date",
        conn,
    )

    if df.empty:
        conn.close()
        return {"tickers_processed": 0, "tickers_skipped": 0}

    processed = 0
    skipped = 0

    with conn:
        for ticker, grp in df.groupby("ticker"):
            closes = grp.sort_values("date")["close"].astype(float).reset_index(drop=True)
            latest_date = grp["date"].max()
            latest_close = float(closes.iloc[-1])

            # Need at least 14 days for RSI
            if len(closes) < 14:
                skipped += 1
                continue

            ma50 = float(closes.rolling(50).mean().iloc[-1]) if len(closes) >= 50 else None
            ma200 = float(closes.rolling(200).mean().iloc[-1]) if len(closes) >= 200 else None
            rsi14 = _rsi(closes)
            mom = _momentum_score(closes)
            trend = _trend_direction(
                latest_close,
                ma50 if ma50 is not None and not pd.isna(ma50) else None,
                ma200 if ma200 is not None and not pd.isna(ma200) else None,
            )

            def _nan_to_none(v):
                return None if v is None or pd.isna(v) else v

            conn.execute(
                """
                INSERT INTO signals
                    (ticker, date, momentum_score, trend_direction, ma_50, ma_200, rsi_14)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker, date) DO UPDATE SET
                    momentum_score  = excluded.momentum_score,
                    trend_direction = excluded.trend_direction,
                    ma_50           = excluded.ma_50,
                    ma_200          = excluded.ma_200,
                    rsi_14          = excluded.rsi_14,
                    computed_at     = datetime('now')
                """,
                (
                    ticker,
                    latest_date,
                    _nan_to_none(mom),
                    trend,
                    _nan_to_none(ma50),
                    _nan_to_none(ma200),
                    _nan_to_none(rsi14),
                ),
            )
            processed += 1

    conn.close()
    return {"tickers_processed": processed, "tickers_skipped": skipped}


def load_latest_signals() -> pd.DataFrame:
    """
    Return a DataFrame with the most recent signal per ticker, joined with
    latest close price and screener_imports data where available.

    Columns returned:
        ticker, company, country, market, sector, industry,
        current_price, signal_date,
        momentum_score, trend_direction, ma_50, ma_200, rsi_14,
        above_ma50, above_ma200, golden_cross,
        pe_current, peg_current, price_ma200_pct, perf_3m, perf_6m, perf_3y,
        roe_avg_3y, net_debt_ebitda, profit_margin,
        earnings_growth_5y, revenue_growth_5y, market_cap_sek
    """
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT
            si.ticker,
            COALESCE(si.company, si.ticker)     AS company,
            si.country,
            si.market,
            si.sector,
            si.industry,
            -- Latest close price from prices table
            lp.close                            AS current_price,
            -- Latest signal
            sig.date                            AS signal_date,
            sig.momentum_score,
            sig.trend_direction,
            sig.ma_50,
            sig.ma_200,
            sig.rsi_14,
            -- Derived boolean flags (computed below in Python)
            lp.close                            AS _close_for_flags,
            sig.ma_50                           AS _ma50_for_flags,
            sig.ma_200                          AS _ma200_for_flags,
            -- Fundamental data from screener_imports
            si.pe_current,
            si.peg_current,
            si.price_ma200_pct,
            si.perf_3m,
            si.perf_6m,
            si.perf_3y,
            si.roe_avg_3y,
            si.net_debt_ebitda,
            si.profit_margin,
            si.earnings_growth_5y,
            si.revenue_growth_5y,
            si.market_cap_sek
        FROM screener_imports si
        LEFT JOIN (
            SELECT ticker, close
            FROM prices
            WHERE (ticker, date) IN (
                SELECT ticker, MAX(date) FROM prices GROUP BY ticker
            )
        ) lp ON lp.ticker = si.ticker
        LEFT JOIN signals sig ON sig.ticker = si.ticker
            AND sig.date = (
                SELECT MAX(date) FROM signals WHERE ticker = si.ticker
            )
        ORDER BY si.ticker
        """,
        conn,
    )
    conn.close()

    if df.empty:
        return df

    # Compute boolean flags in Python
    df["above_ma50"] = (
        df["_close_for_flags"].notna()
        & df["_ma50_for_flags"].notna()
        & (df["_close_for_flags"] > df["_ma50_for_flags"])
    )
    df["above_ma200"] = (
        df["_close_for_flags"].notna()
        & df["_ma200_for_flags"].notna()
        & (df["_close_for_flags"] > df["_ma200_for_flags"])
    )
    df["golden_cross"] = (
        df["_ma50_for_flags"].notna()
        & df["_ma200_for_flags"].notna()
        & (df["_ma50_for_flags"] > df["_ma200_for_flags"])
    )

    return df.drop(columns=["_close_for_flags", "_ma50_for_flags", "_ma200_for_flags"])
