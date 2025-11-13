"""Streamlit UI for the LFPI live tracker."""

from __future__ import annotations

import asyncio
import re
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Tuple
from uuid import uuid4

import pandas as pd
import streamlit as st
from dateutil import parser as date_parser
from zoneinfo import ZoneInfo, available_timezones

import LiveTickerFinal23092025 as tracker

BUNDLED_CSV_PATH = Path(__file__).with_name("websitelinks.csv")
LOCAL_TZ = datetime.now().astimezone().tzinfo or timezone.utc
LOCAL_TZ_KEY = getattr(LOCAL_TZ, "key", LOCAL_TZ.tzname(None) if LOCAL_TZ else "UTC") or "UTC"
TIMEZONE_OPTIONS = ["Source timezone (no conversion)"] + sorted(available_timezones())
TZINFOS = {"UTC": timezone.utc, "GMT": timezone.utc}
for offset in range(-12, 15):
    tz = timezone(timedelta(hours=offset))
    label = f"GMT{offset:+d}"
    TZINFOS[label] = tz
    TZINFOS[f"{label}:00"] = tz


def _bool_from_secret(value, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "on"}


def password_required() -> bool:
    return _bool_from_secret(st.secrets.get("require_password", True), True)


def password_gate() -> bool:
    if not password_required():
        return True

    if "auth_ok" not in st.session_state:
        st.session_state.auth_ok = False

    if st.session_state.auth_ok:
        return True

    pwd = st.text_input("Password", type="password")
    if st.button("Unlock", type="primary"):
        expected = st.secrets.get("app_password", "")
        st.session_state.auth_ok = bool(pwd) and (pwd == expected)
        if not st.session_state.auth_ok:
            st.error("Incorrect password.")
    return st.session_state.auth_ok


def _persist_uploaded_file(uploaded) -> str:
    suffix = Path(uploaded.name).suffix or ".csv"
    tmp_path = Path(tempfile.gettempdir()) / f"lfpi_csv_{uuid4().hex}{suffix}"
    tmp_path.write_bytes(uploaded.getbuffer())
    return str(tmp_path)


def _load_and_fetch(csv_path: str):
    tracker.load_config_from_local_csv(csv_path)
    return asyncio.run(tracker.gather_all_data())


def _extract_dt_fragment(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    fragment = re.split(r"[|·]", text, maxsplit=1)[0].strip()
    fragment = fragment.replace("GMT+ ", "GMT+").replace("GMT- ", "GMT-")
    return fragment


def _normalize_datetime(raw: str, target_tz: Optional[ZoneInfo]) -> Tuple[str, str, str]:
    fragment = _extract_dt_fragment(raw)
    if not fragment:
        return "", "", ""
    tz_match = re.search(r"(GMT[+\-]\d+(?::\d+)?|GMT|UTC)", fragment, flags=re.IGNORECASE)
    tz_label = tz_match.group(1).upper() if tz_match else ""
    tz_label = tz_label.replace("UTC", "GMT")
    try:
        dt = date_parser.parse(fragment, fuzzy=True, tzinfos=TZINFOS)
    except Exception:
        return fragment, "", tz_label
    source_tz = TZINFOS.get(tz_label)
    if source_tz is not None:
        dt = dt.replace(tzinfo=source_tz)
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ or timezone.utc)
    if target_tz:
        dt = dt.astimezone(target_tz)
        tz_display = target_tz.key if hasattr(target_tz, "key") else (target_tz.tzname(dt) or "")
    else:
        tz_display = tz_label or (dt.tzname() or "")
    date_part = dt.strftime("%d-%m-%Y")
    time_part = dt.strftime("%H:%M:%S")
    return date_part, time_part, tz_display


def _format_equities(df: pd.DataFrame, target_tz: Optional[ZoneInfo]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Ticker", "Price", "Date", "Time", "Exchange", "Currency"])
    view = df.copy()
    view["PRICE"] = pd.to_numeric(view["PRICE"], errors="coerce").round(2)
    date_time = view["DATE"].apply(lambda val: _normalize_datetime(val, target_tz))
    view["DATE"] = date_time.apply(lambda x: x[0])
    view["TIME"] = date_time.apply(lambda x: x[1])
    view = view[["TICKER", "PRICE", "DATE", "TIME", "EXCHANGE", "CURRENCY"]]
    view.columns = ["Ticker", "Price", "Date", "Time", "Exchange", "Currency"]
    return view


def _format_forex(df: pd.DataFrame, target_tz: Optional[ZoneInfo]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Ticker", "Price", "Date", "Time"])
    view = df.copy()
    view["PRICE"] = pd.to_numeric(view["PRICE"], errors="coerce").round(4)
    date_time = view["DATE"].apply(lambda val: _normalize_datetime(val, target_tz))
    view["DATE"] = date_time.apply(lambda x: x[0])
    view["TIME"] = date_time.apply(lambda x: x[1])
    view = view[["TICKER", "PRICE", "DATE", "TIME"]]
    view.columns = ["Ticker", "Price", "Date", "Time"]
    return view


def _render_table_section(title: str, df: pd.DataFrame, download_label: str, filename: str, price_format: str):
    left, center, right = st.columns([1, 4, 1])
    with center:
        st.subheader(title)
        st.dataframe(
            df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Price": st.column_config.NumberColumn("Price", format=price_format),
            },
        )
        st.download_button(
            download_label,
            df.to_csv(index=False).encode("utf-8"),
            file_name=filename,
            mime="text/csv",
            use_container_width=True,
        )


def _combined_ticker_price(eq_df: pd.DataFrame, fx_df: pd.DataFrame) -> pd.DataFrame:
    frames = []
    if eq_df is not None and not eq_df.empty:
        eq_simple = eq_df[["Ticker", "Price"]].copy()
        eq_simple["Dataset"] = "Equities"
        frames.append(eq_simple)
    if fx_df is not None and not fx_df.empty:
        fx_simple = fx_df[["Ticker", "Price"]].copy()
        fx_simple["Dataset"] = "Forex"
        frames.append(fx_simple)
    if not frames:
        return pd.DataFrame(columns=["Dataset", "Ticker", "Price"])
    combined = pd.concat(frames, ignore_index=True)[["Dataset", "Ticker", "Price"]]
    return combined


def _render_timings(timings: dict):
    if not timings:
        return
    data = pd.DataFrame(
        [{"Section": key, "Duration": value} for key, value in timings.items()]
    )
    left, center, right = st.columns([1, 4, 1])
    with center:
        st.dataframe(
            data.style.hide(axis="index").set_properties(**{"text-align": "center"}),
            use_container_width=True,
            hide_index=True,
        )


def main():
    st.set_page_config(
        page_title="LFPI Live Terminal",
        page_icon="📈",
        layout="wide",
    )

    st.title("LFPI Live Terminal")
    st.caption("Streamlit web UI powered by LiveTickerFinal23092025.")
    st.markdown(
        """
        <style>
        div[data-testid="stDataFrame"] table td,
        div[data-testid="stDataFrame"] table th {
            text-align: center !important;
        }
        div[data-testid="stDataFrame"] div[role="gridcell"],
        div[data-testid="stDataFrame"] div[role="columnheader"] {
            display: flex !important;
            justify-content: center !important;
            align-items: center !important;
            text-align: center !important;
        }
        div[data-testid="stDataFrame"] div[role="gridcell"] > div,
        div[data-testid="stDataFrame"] div[role="columnheader"] > div {
            width: 100% !important;
            display: flex !important;
            justify-content: center !important;
            align-items: center !important;
            text-align: center !important;
        }
        div[data-testid="stDataFrame"] div[role="gridcell"] span,
        div[data-testid="stDataFrame"] div[role="columnheader"] span {
            width: 100% !important;
            text-align: center !important;
            display: flex !important;
            justify-content: center !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if not password_gate():
        st.stop()

    csv_choice = st.radio(
        "Configuration source",
        options=("Bundled CSV", "Upload CSV"),
        horizontal=True,
    )

    csv_path: Optional[str] = None
    csv_label = ""

    if csv_choice == "Bundled CSV":
        if not BUNDLED_CSV_PATH.exists():
            st.error(f"Bundled CSV not found at {BUNDLED_CSV_PATH}")
            st.stop()
        csv_path = str(BUNDLED_CSV_PATH)
        csv_label = f"Bundled file ({BUNDLED_CSV_PATH.name})"
        st.caption(f"Using bundled file: `{BUNDLED_CSV_PATH}`")
    else:
        uploaded = st.file_uploader(
            "Upload configuration file (CSV or Excel)",
            type=["csv", "xlsx", "xls"],
        )
        if not uploaded:
            st.info("Upload a configuration file to continue.")
            st.stop()
        csv_path = _persist_uploaded_file(uploaded)
        csv_label = f"Uploaded file ({uploaded.name})"
        st.caption(f"Uploaded file stored temporarily at `{csv_path}`")

    control_left, control_center, control_right = st.columns([1, 4, 1])
    with control_center:
        tz_choice = st.selectbox(
            "Display timestamps in",
            TIMEZONE_OPTIONS,
            index=0,
            help="Choose a timezone to convert all timestamps. Select the first option to keep source timestamps.",
        )
        target_tz = None if tz_choice == "Source timezone (no conversion)" else ZoneInfo(tz_choice)
        if target_tz:
            st.caption(f"Time column shown in: {tz_choice}")
        else:
            st.caption(f"Time column shown in source timezone (local detected: {LOCAL_TZ_KEY}).")

        action_col1, action_col2 = st.columns(2)
        fetch_clicked = action_col1.button("Fetch / Refresh", type="primary", use_container_width=True)
        if action_col2.button("Clear results", use_container_width=True):
            for key in ("latest_data", "latest_updated", "latest_source"):
                st.session_state.pop(key, None)
            st.info("Cleared previous results.")

    if fetch_clicked and csv_path:
        try:
            with st.spinner("Loading configuration..."):
                data = _load_and_fetch(csv_path)
            st.session_state["latest_data"] = data
            st.session_state["latest_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.session_state["latest_source"] = csv_label
        except Exception as exc:  # noqa: BLE001
            st.exception(exc)

    latest_data = st.session_state.get("latest_data")
    if not latest_data:
        st.stop()

    st.success(
        f"Last refreshed at {st.session_state.get('latest_updated')} using {st.session_state.get('latest_source')}."
    )

    warnings = latest_data.get("warnings") or []
    if warnings:
        st.warning("Warnings:\n" + "\n".join(f"- {msg}" for msg in warnings))

    timings = latest_data.get("timings")
    st.subheader("Timings")
    _render_timings(timings)

    metrics_col1, metrics_col2 = st.columns(2)
    metrics_col1.metric("Equities rows", latest_data.get("eq_count", 0))
    metrics_col2.metric("Forex rows", latest_data.get("fx_count", 0))

    equities_view = _format_equities(latest_data["equities"], target_tz)
    _render_table_section(
        "Equities",
        equities_view,
        "Download equities CSV",
        "equities.csv",
        "%.2f",
    )

    forex_view = _format_forex(latest_data["forex"], target_tz)
    _render_table_section(
        "Forex",
        forex_view,
        "Download forex CSV",
        "forex.csv",
        "%.4f",
    )

    combined_simple = _combined_ticker_price(equities_view, forex_view)
    left, center, right = st.columns([1, 4, 1])
    with center:
        st.download_button(
            "Download all tickers (Ticker + Price)",
            combined_simple.to_csv(index=False).encode("utf-8"),
            file_name="all_tickers_prices.csv",
            mime="text/csv",
            use_container_width=True,
        )


if __name__ == "__main__":
    main()
