"""
pages/screener_page.py — Momentum & Trend Screener.

Two tabs:
  1. Stock Screener  — view and filter all stocks; apply saved screen presets.
  2. Screening Setup — create, edit, and delete named screening configurations.
"""

import streamlit as st
import pandas as pd

from database import get_connection
from components.screener_engine import run_screener, load_latest_signals


# ── DB helpers for screen_configs ─────────────────────────────────────────────

def _load_screen_configs() -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM screen_configs ORDER BY name"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _save_screen_config(cfg: dict) -> None:
    """Insert or update a screen config (matched by name)."""
    cols = [
        "name", "description",
        "trend_direction", "min_momentum_score", "max_momentum_score",
        "min_rsi", "max_rsi",
        "require_above_ma50", "require_above_ma200", "require_golden_cross",
        "max_pe", "min_roe", "max_net_debt_ebitda", "min_profit_margin",
        "min_perf_3m", "min_perf_6m",
        "min_earnings_growth_5y", "min_revenue_growth_5y",
        "min_price_ma200_pct", "max_price_ma200_pct",
        "country", "market", "sector",
    ]
    values = [cfg.get(c) for c in cols]
    placeholders = ", ".join(["?"] * len(cols))
    update_str = ", ".join(
        f"{c} = excluded.{c}" for c in cols if c != "name"
    )
    sql = (
        f"INSERT INTO screen_configs ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(name) DO UPDATE SET {update_str}, updated_at = datetime('now')"
    )
    conn = get_connection()
    with conn:
        conn.execute(sql, values)
    conn.close()


def _delete_screen_config(config_id: int) -> None:
    conn = get_connection()
    with conn:
        conn.execute("DELETE FROM screen_configs WHERE id = ?", (config_id,))
    conn.close()


# ── Filtering helpers ─────────────────────────────────────────────────────────

def _none_if_empty(v):
    """Convert empty string to None for optional filter values."""
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return None
    return v


def _apply_filters(df: pd.DataFrame, f: dict) -> pd.DataFrame:
    """
    Apply a flat filters dict to the stocks DataFrame.
    All filter keys are optional — missing or None values are ignored.
    """
    out = df.copy()

    def _num_filter(col, min_val=None, max_val=None):
        nonlocal out
        if min_val is not None and col in out.columns:
            out = out[out[col].isna() | (out[col] >= min_val)]
        if max_val is not None and col in out.columns:
            out = out[out[col].isna() | (out[col] <= max_val)]

    # Universe
    if f.get("country"):
        out = out[out["country"] == f["country"]]
    if f.get("market"):
        out = out[out["market"] == f["market"]]
    if f.get("sector"):
        out = out[out["sector"] == f["sector"]]

    # Trend
    if f.get("trend_direction"):
        out = out[out["trend_direction"] == f["trend_direction"]]

    # Technical
    _num_filter("momentum_score", f.get("min_momentum_score"), f.get("max_momentum_score"))
    _num_filter("rsi_14", f.get("min_rsi"), f.get("max_rsi"))

    if f.get("require_above_ma50"):
        out = out[out["above_ma50"] == True]  # noqa: E712
    if f.get("require_above_ma200"):
        out = out[out["above_ma200"] == True]  # noqa: E712
    if f.get("require_golden_cross"):
        out = out[out["golden_cross"] == True]  # noqa: E712

    # Fundamental
    _num_filter("pe_current",          max_val=f.get("max_pe"))
    _num_filter("roe_avg_3y",          min_val=f.get("min_roe"))
    _num_filter("net_debt_ebitda",     max_val=f.get("max_net_debt_ebitda"))
    _num_filter("profit_margin",       min_val=f.get("min_profit_margin"))
    _num_filter("perf_3m",             min_val=f.get("min_perf_3m"))
    _num_filter("perf_6m",             min_val=f.get("min_perf_6m"))
    _num_filter("earnings_growth_5y",  min_val=f.get("min_earnings_growth_5y"))
    _num_filter("revenue_growth_5y",   min_val=f.get("min_revenue_growth_5y"))
    _num_filter("price_ma200_pct",     min_val=f.get("min_price_ma200_pct"),
                                       max_val=f.get("max_price_ma200_pct"))

    return out


def _config_to_filters(cfg: dict) -> dict:
    """Convert a screen_configs DB row to the filters dict format."""
    return {
        "country":               cfg.get("country"),
        "market":                cfg.get("market"),
        "sector":                cfg.get("sector"),
        "trend_direction":       cfg.get("trend_direction"),
        "min_momentum_score":    cfg.get("min_momentum_score"),
        "max_momentum_score":    cfg.get("max_momentum_score"),
        "min_rsi":               cfg.get("min_rsi"),
        "max_rsi":               cfg.get("max_rsi"),
        "require_above_ma50":    bool(cfg.get("require_above_ma50")),
        "require_above_ma200":   bool(cfg.get("require_above_ma200")),
        "require_golden_cross":  bool(cfg.get("require_golden_cross")),
        "max_pe":                cfg.get("max_pe"),
        "min_roe":               cfg.get("min_roe"),
        "max_net_debt_ebitda":   cfg.get("max_net_debt_ebitda"),
        "min_profit_margin":     cfg.get("min_profit_margin"),
        "min_perf_3m":           cfg.get("min_perf_3m"),
        "min_perf_6m":           cfg.get("min_perf_6m"),
        "min_earnings_growth_5y": cfg.get("min_earnings_growth_5y"),
        "min_revenue_growth_5y": cfg.get("min_revenue_growth_5y"),
        "min_price_ma200_pct":   cfg.get("min_price_ma200_pct"),
        "max_price_ma200_pct":   cfg.get("max_price_ma200_pct"),
    }


# ── Screener config form (shared by create and edit) ─────────────────────────

def _config_form(form_key: str, prefill: dict | None = None) -> dict | None:
    """
    Render the create/edit form for a screen config.
    Returns the submitted config dict, or None if form was not submitted.
    prefill is a screen_configs DB row dict for edit mode.
    """
    p = prefill or {}

    def _pf(key, default=None):
        v = p.get(key, default)
        return v if v is not None else default

    with st.form(form_key):
        st.markdown("**Basic info**")
        c1, c2 = st.columns([1, 2])
        with c1:
            name = st.text_input("Screen name *", value=_pf("name", ""))
        with c2:
            description = st.text_input("Description", value=_pf("description", ""))

        st.markdown("---")
        st.markdown("**Universe filters**")
        u1, u2, u3 = st.columns(3)
        with u1:
            country = st.text_input("Country (exact match)", value=_pf("country", ""))
        with u2:
            market = st.text_input("Market (exact match)", value=_pf("market", ""))
        with u3:
            sector = st.text_input("Sector (exact match)", value=_pf("sector", ""))

        st.markdown("---")
        st.markdown("**Technical / signal criteria**")

        t1, t2, t3 = st.columns(3)
        with t1:
            trend_opts = ["(any)", "up", "down", "sideways"]
            current_trend = _pf("trend_direction", "(any)") or "(any)"
            trend_direction = st.selectbox(
                "Trend direction", trend_opts,
                index=trend_opts.index(current_trend) if current_trend in trend_opts else 0,
            )
        with t2:
            min_momentum = st.number_input(
                "Min momentum score", value=float(_pf("min_momentum_score") or 0.0),
                step=1.0, format="%.1f",
                help="Leave 0 to skip",
            )
        with t3:
            max_momentum = st.number_input(
                "Max momentum score", value=float(_pf("max_momentum_score") or 0.0),
                step=1.0, format="%.1f",
                help="Leave 0 to skip",
            )

        r1, r2 = st.columns(2)
        with r1:
            min_rsi = st.number_input(
                "Min RSI", min_value=0.0, max_value=100.0,
                value=float(_pf("min_rsi") or 0.0), step=1.0, format="%.0f",
                help="0 = no minimum",
            )
        with r2:
            max_rsi = st.number_input(
                "Max RSI", min_value=0.0, max_value=100.0,
                value=float(_pf("max_rsi") or 100.0), step=1.0, format="%.0f",
                help="100 = no maximum",
            )

        ma1, ma2, ma3 = st.columns(3)
        with ma1:
            above_ma50 = st.checkbox("Must be above MA-50", value=bool(_pf("require_above_ma50", False)))
        with ma2:
            above_ma200 = st.checkbox("Must be above MA-200", value=bool(_pf("require_above_ma200", False)))
        with ma3:
            golden_cross = st.checkbox("Golden cross (MA50 > MA200)", value=bool(_pf("require_golden_cross", False)))

        st.markdown("---")
        st.markdown("**Fundamental / Börsdata criteria**")

        f1, f2, f3 = st.columns(3)
        with f1:
            max_pe = st.number_input("Max P/E", value=float(_pf("max_pe") or 0.0), step=1.0, format="%.1f", help="0 = no limit")
            min_roe = st.number_input("Min ROE %", value=float(_pf("min_roe") or 0.0), step=1.0, format="%.1f", help="0 = no minimum")
        with f2:
            max_nd_ebitda = st.number_input("Max Net Debt/EBITDA", value=float(_pf("max_net_debt_ebitda") or 0.0), step=0.5, format="%.1f", help="0 = no limit")
            min_profit_margin = st.number_input("Min Profit Margin %", value=float(_pf("min_profit_margin") or 0.0), step=1.0, format="%.1f", help="0 = no minimum")
        with f3:
            min_perf_3m = st.number_input("Min 3m Performance %", value=float(_pf("min_perf_3m") or 0.0), step=1.0, format="%.1f", help="0 = no minimum")
            min_perf_6m = st.number_input("Min 6m Performance %", value=float(_pf("min_perf_6m") or 0.0), step=1.0, format="%.1f", help="0 = no minimum")

        g1, g2 = st.columns(2)
        with g1:
            min_eps_growth = st.number_input("Min EPS Growth 5y %", value=float(_pf("min_earnings_growth_5y") or 0.0), step=1.0, format="%.1f", help="0 = no minimum")
        with g2:
            min_rev_growth = st.number_input("Min Revenue Growth 5y %", value=float(_pf("min_revenue_growth_5y") or 0.0), step=1.0, format="%.1f", help="0 = no minimum")

        submitted = st.form_submit_button("Save screen", type="primary")

    if not submitted:
        return None

    if not name.strip():
        st.error("Screen name is required.")
        return None

    def _opt(v, zero_means_none=True):
        """Return None if value is falsy/zero (user left at default 0)."""
        if zero_means_none and v == 0.0:
            return None
        return v or None

    return {
        "name":                     name.strip(),
        "description":              description.strip() or None,
        "country":                  _none_if_empty(country),
        "market":                   _none_if_empty(market),
        "sector":                   _none_if_empty(sector),
        "trend_direction":          None if trend_direction == "(any)" else trend_direction,
        "min_momentum_score":       _opt(min_momentum),
        "max_momentum_score":       _opt(max_momentum),
        "min_rsi":                  _opt(min_rsi),
        "max_rsi":                  None if max_rsi == 100.0 else max_rsi,
        "require_above_ma50":       int(above_ma50),
        "require_above_ma200":      int(above_ma200),
        "require_golden_cross":     int(golden_cross),
        "max_pe":                   _opt(max_pe),
        "min_roe":                  _opt(min_roe),
        "max_net_debt_ebitda":      _opt(max_nd_ebitda),
        "min_profit_margin":        _opt(min_profit_margin),
        "min_perf_3m":              _opt(min_perf_3m),
        "min_perf_6m":              _opt(min_perf_6m),
        "min_earnings_growth_5y":   _opt(min_eps_growth),
        "min_revenue_growth_5y":    _opt(min_rev_growth),
        "min_price_ma200_pct":      None,
        "max_price_ma200_pct":      None,
    }


# ── Tab 1: Stock Screener ─────────────────────────────────────────────────────

def _render_stock_screener(df: pd.DataFrame, configs: list[dict]) -> None:
    # ── Preset selector ───────────────────────────────────────────────────────
    preset_names = ["(manual filters)"] + [c["name"] for c in configs]
    preset_choice = st.selectbox(
        "Apply saved screen preset",
        preset_names,
        help="Select a saved screening configuration to pre-populate the filters below.",
    )

    preset_filters: dict = {}
    if preset_choice != "(manual filters)":
        selected_cfg = next(c for c in configs if c["name"] == preset_choice)
        preset_filters = _config_to_filters(selected_cfg)
        st.info(
            f"**{selected_cfg['name']}**"
            + (f" — {selected_cfg['description']}" if selected_cfg.get("description") else "")
        )

    st.divider()

    # ── Run screener engine button ────────────────────────────────────────────
    col_run, col_status = st.columns([1, 4])
    with col_run:
        if st.button("Run screener engine", type="primary",
                     help="Compute RSI, moving averages and momentum scores from imported price data."):
            with st.spinner("Computing signals…"):
                result = run_screener()
            st.success(
                f"Done — {result['tickers_processed']} tickers updated, "
                f"{result['tickers_skipped']} skipped (insufficient history)."
            )
            st.rerun()

    if df.empty:
        st.info(
            "No stock data available. Go to **Import** and upload a Börsdata screener export first."
        )
        return

    # ── Manual filters (pre-populated from preset if selected) ───────────────
    with st.expander("Filters", expanded=True):
        # Universe
        st.markdown("**Universe**")
        uf1, uf2, uf3 = st.columns(3)
        with uf1:
            countries = ["All"] + sorted(df["country"].dropna().unique().tolist())
            pv_country = preset_filters.get("country") or "All"
            country_f = st.selectbox("Country", countries,
                                     index=countries.index(pv_country) if pv_country in countries else 0,
                                     key="sc_country")
        with uf2:
            markets = ["All"] + sorted(df["market"].dropna().unique().tolist())
            pv_market = preset_filters.get("market") or "All"
            market_f = st.selectbox("Market", markets,
                                    index=markets.index(pv_market) if pv_market in markets else 0,
                                    key="sc_market")
        with uf3:
            sectors = ["All"] + sorted(df["sector"].dropna().unique().tolist())
            pv_sector = preset_filters.get("sector") or "All"
            sector_f = st.selectbox("Sector", sectors,
                                    index=sectors.index(pv_sector) if pv_sector in sectors else 0,
                                    key="sc_sector")

        st.markdown("**Technical**")
        tc1, tc2, tc3 = st.columns(3)
        with tc1:
            trend_opts = ["All", "up", "down", "sideways"]
            pv_trend = preset_filters.get("trend_direction") or "All"
            trend_f = st.selectbox("Trend", trend_opts,
                                   index=trend_opts.index(pv_trend) if pv_trend in trend_opts else 0,
                                   key="sc_trend")
        with tc2:
            min_mom = st.number_input("Min momentum score", value=float(preset_filters.get("min_momentum_score") or 0.0),
                                      step=1.0, format="%.1f", key="sc_min_mom")
        with tc3:
            rsi_col1, rsi_col2 = st.columns(2)
            with rsi_col1:
                min_rsi = st.number_input("RSI min", min_value=0.0, max_value=100.0,
                                          value=float(preset_filters.get("min_rsi") or 0.0),
                                          step=1.0, format="%.0f", key="sc_rsi_min")
            with rsi_col2:
                max_rsi = st.number_input("RSI max", min_value=0.0, max_value=100.0,
                                          value=float(preset_filters.get("max_rsi") or 100.0),
                                          step=1.0, format="%.0f", key="sc_rsi_max")

        ma_c1, ma_c2, ma_c3 = st.columns(3)
        with ma_c1:
            above_ma50_f = st.checkbox("Above MA-50", value=bool(preset_filters.get("require_above_ma50")), key="sc_ma50")
        with ma_c2:
            above_ma200_f = st.checkbox("Above MA-200", value=bool(preset_filters.get("require_above_ma200")), key="sc_ma200")
        with ma_c3:
            golden_cross_f = st.checkbox("Golden cross", value=bool(preset_filters.get("require_golden_cross")), key="sc_gc")

        st.markdown("**Fundamental**")
        ff1, ff2, ff3 = st.columns(3)
        with ff1:
            max_pe_f = st.number_input("Max P/E", value=float(preset_filters.get("max_pe") or 0.0),
                                       step=1.0, format="%.1f", key="sc_pe",
                                       help="0 = no limit")
            min_roe_f = st.number_input("Min ROE %", value=float(preset_filters.get("min_roe") or 0.0),
                                        step=1.0, format="%.1f", key="sc_roe",
                                        help="0 = no minimum")
        with ff2:
            max_nd_f = st.number_input("Max Net Debt/EBITDA", value=float(preset_filters.get("max_net_debt_ebitda") or 0.0),
                                       step=0.5, format="%.1f", key="sc_nd",
                                       help="0 = no limit")
            min_pm_f = st.number_input("Min Profit Margin %", value=float(preset_filters.get("min_profit_margin") or 0.0),
                                       step=1.0, format="%.1f", key="sc_pm",
                                       help="0 = no minimum")
        with ff3:
            min_p3m_f = st.number_input("Min 3m Perf %", value=float(preset_filters.get("min_perf_3m") or 0.0),
                                        step=1.0, format="%.1f", key="sc_p3m",
                                        help="0 = no minimum")
            min_p6m_f = st.number_input("Min 6m Perf %", value=float(preset_filters.get("min_perf_6m") or 0.0),
                                        step=1.0, format="%.1f", key="sc_p6m",
                                        help="0 = no minimum")

        st.markdown("**Sort by**")
        sort_options = {
            "Momentum score ↓": ("momentum_score", False),
            "3m Performance ↓": ("perf_3m", False),
            "6m Performance ↓": ("perf_6m", False),
            "RSI ↑": ("rsi_14", True),
            "RSI ↓": ("rsi_14", False),
            "P/E ↑": ("pe_current", True),
            "ROE ↓": ("roe_avg_3y", False),
            "Ticker A→Z": ("ticker", True),
        }
        sort_label = st.selectbox("Sort by", list(sort_options.keys()), key="sc_sort")
        sort_col, sort_asc = sort_options[sort_label]

    # ── Build filter dict from manual controls ────────────────────────────────
    filters = {
        "country":              None if country_f == "All" else country_f,
        "market":               None if market_f == "All" else market_f,
        "sector":               None if sector_f == "All" else sector_f,
        "trend_direction":      None if trend_f == "All" else trend_f,
        "min_momentum_score":   min_mom if min_mom != 0.0 else None,
        "min_rsi":              min_rsi if min_rsi != 0.0 else None,
        "max_rsi":              max_rsi if max_rsi != 100.0 else None,
        "require_above_ma50":   above_ma50_f,
        "require_above_ma200":  above_ma200_f,
        "require_golden_cross": golden_cross_f,
        "max_pe":               max_pe_f if max_pe_f != 0.0 else None,
        "min_roe":              min_roe_f if min_roe_f != 0.0 else None,
        "max_net_debt_ebitda":  max_nd_f if max_nd_f != 0.0 else None,
        "min_profit_margin":    min_pm_f if min_pm_f != 0.0 else None,
        "min_perf_3m":          min_p3m_f if min_p3m_f != 0.0 else None,
        "min_perf_6m":          min_p6m_f if min_p6m_f != 0.0 else None,
    }

    filtered = _apply_filters(df, filters)

    # Sort
    if sort_col in filtered.columns:
        filtered = filtered.sort_values(sort_col, ascending=sort_asc, na_position="last")

    st.caption(f"Showing **{len(filtered)}** of {len(df)} stocks · sorted by {sort_label}")

    # ── Display table ─────────────────────────────────────────────────────────
    display_cols_ordered = [
        "ticker", "company", "country", "market", "sector",
        "trend_direction", "momentum_score", "rsi_14", "ma_50", "ma_200",
        "above_ma50", "above_ma200", "golden_cross",
        "pe_current", "roe_avg_3y", "net_debt_ebitda", "profit_margin",
        "perf_3m", "perf_6m", "perf_3y",
        "earnings_growth_5y", "revenue_growth_5y",
        "price_ma200_pct", "market_cap_sek",
    ]
    display_cols = [c for c in display_cols_ordered if c in filtered.columns]

    col_labels = {
        "ticker": "Ticker", "company": "Company", "country": "Country",
        "market": "Market", "sector": "Sector",
        "trend_direction": "Trend", "momentum_score": "Momentum",
        "rsi_14": "RSI 14", "ma_50": "MA 50", "ma_200": "MA 200",
        "above_ma50": "↑MA50", "above_ma200": "↑MA200", "golden_cross": "GX",
        "pe_current": "P/E", "roe_avg_3y": "ROE 3y%", "net_debt_ebitda": "ND/EBITDA",
        "profit_margin": "Margin%", "perf_3m": "3m%", "perf_6m": "6m%", "perf_3y": "3y%",
        "earnings_growth_5y": "EPS5y%", "revenue_growth_5y": "Rev5y%",
        "price_ma200_pct": "P/MA200%", "market_cap_sek": "MktCap SEK",
    }

    display = filtered[display_cols].rename(columns=col_labels)

    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Trend": st.column_config.TextColumn("Trend"),
            "Momentum": st.column_config.NumberColumn("Momentum", format="%.1f"),
            "RSI 14": st.column_config.NumberColumn("RSI 14", format="%.1f"),
            "3m%": st.column_config.NumberColumn("3m%", format="%.1f"),
            "6m%": st.column_config.NumberColumn("6m%", format="%.1f"),
        },
    )


# ── Tab 2: Screening Setup ────────────────────────────────────────────────────

def _render_screening_setup(configs: list[dict]) -> None:
    st.markdown(
        "Define named screening presets. Each preset stores a set of criteria "
        "that can be applied instantly on the **Stock Screener** tab."
    )

    # ── Existing configs ──────────────────────────────────────────────────────
    if configs:
        st.subheader(f"Saved screens ({len(configs)})")
        for cfg in configs:
            active_criteria = []
            if cfg.get("trend_direction"):
                active_criteria.append(f"Trend: {cfg['trend_direction']}")
            if cfg.get("min_momentum_score"):
                active_criteria.append(f"Mom ≥ {cfg['min_momentum_score']}")
            if cfg.get("max_pe"):
                active_criteria.append(f"P/E ≤ {cfg['max_pe']}")
            if cfg.get("min_roe"):
                active_criteria.append(f"ROE ≥ {cfg['min_roe']}%")
            if cfg.get("require_golden_cross"):
                active_criteria.append("Golden cross")
            if cfg.get("require_above_ma200"):
                active_criteria.append("Above MA-200")
            if cfg.get("min_perf_3m"):
                active_criteria.append(f"3m perf ≥ {cfg['min_perf_3m']}%")
            if cfg.get("country"):
                active_criteria.append(f"Country: {cfg['country']}")
            if cfg.get("sector"):
                active_criteria.append(f"Sector: {cfg['sector']}")

            summary = " · ".join(active_criteria) if active_criteria else "No active criteria"

            with st.expander(f"**{cfg['name']}** — {cfg.get('description') or summary}"):
                st.caption(summary)

                col_edit, col_del = st.columns([1, 1])
                with col_edit:
                    if st.button("Edit", key=f"edit_{cfg['id']}"):
                        st.session_state[f"editing_{cfg['id']}"] = True

                with col_del:
                    if st.button("Delete", key=f"del_{cfg['id']}", type="secondary"):
                        _delete_screen_config(cfg["id"])
                        st.success(f"Deleted '{cfg['name']}'.")
                        st.rerun()

                if st.session_state.get(f"editing_{cfg['id']}"):
                    st.markdown("**Edit screen**")
                    result = _config_form(f"edit_form_{cfg['id']}", prefill=cfg)
                    if result is not None:
                        _save_screen_config(result)
                        st.session_state.pop(f"editing_{cfg['id']}", None)
                        st.success(f"Saved '{result['name']}'.")
                        st.rerun()
    else:
        st.info("No screens saved yet. Create your first one below.")

    # ── Create new ────────────────────────────────────────────────────────────
    st.divider()
    st.subheader("Create new screen")
    result = _config_form("create_form")
    if result is not None:
        try:
            _save_screen_config(result)
            st.success(f"Screen **{result['name']}** saved.")
            st.rerun()
        except Exception as e:
            st.error(f"Could not save: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────

def render() -> None:
    st.header("Screening Engine")

    configs = _load_screen_configs()
    df = load_latest_signals()

    tab_screener, tab_setup = st.tabs(["Stock Screener", "Screening Setup"])

    with tab_screener:
        _render_stock_screener(df, configs)

    with tab_setup:
        _render_screening_setup(configs)
