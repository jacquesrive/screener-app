# =========================================================
# LFPI PORTFOLIO + FOREX  -  Google Finance + Euronext + Zone Bourse + TradingView FX
# Loads ALL inputs from a local CSV: Z:\Stagiaire\CODE\Live Price Tracker Final\websitelinks.csv
# Writes two tables into one new worksheet of an existing Excel file:
#   1) Combined equities table
#   2) Blank line
#   3) FOREX RATES table (from TradingView public scanner)
# Keeps LSE prices in GBX
# =========================================================

import re, os, sys, time, asyncio, unicodedata, random, importlib, json
from typing import List, Dict, Any, Tuple
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests

# Excel COM (optional)
try:
    import win32com.client as win32
    import pythoncom
    HAS_COM = True
except Exception:
    win32 = None
    pythoncom = None
    HAS_COM = False

# Playwright (optional)
try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except Exception:
    async_playwright = None
    HAS_PLAYWRIGHT = False

# Windows needs Proactor loop for subprocess (Playwright)
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass

# --------------- CONFIG SOURCE -------------------
CONFIG_CSV_PATH = r"Z:\Stagiaire\CODE\Live Price Tracker Final\websitelinks.csv"

# Expected CSV columns:
# source, symbol, url, ticker, zb_exchange
# google uses symbol
# euronext uses url and ticker
# zonebourse uses url, ticker, zb_exchange
# fx uses symbol for the pair like EURUSD
# -------------------------------------------------

# --------------- DEFAULTS AND GLOBALS -------------
PARIS = ZoneInfo("Europe/Paris")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

EXCHANGE_TO_CURRENCY = {
    "LON": "GBX",
    "NASDAQ": "USD",
    "NYSE": "USD",
    "PAR": "EUR", "BRU": "EUR", "AMS": "EUR", "LIS": "EUR",
    "MAD": "EUR", "DUB": "EUR", "BIT": "EUR", "MIL": "EUR", "BMI": "EUR",
    "SIX": "CHF",
}

GOOGLE_SYMBOLS: List[str] = []
EURONEXT_BATCH: List[Dict[str, str]] = []
ZB_STOCKS: List[Tuple[str, str]] = []
ZB_EXCHANGES: Dict[str, str] = {}
FX_PAIRS: List[str] = []  # for TradingView FX
YAHOO_TICKERS: Dict[str, str] = {}

# ================== COMMON HELPERS ==================
def fmt_dur(seconds: float) -> str:
    whole = int(seconds); ms = int(round((seconds - whole) * 1000))
    h = whole // 3600; m = (whole % 3600) // 60; s = whole % 60
    return f"{h}:{m:02d}:{s:02d}.{ms:03d}"

def norm_ws(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

def to_float_any(s: str) -> float | None:
    s = norm_ws(s or "")
    s = re.sub(r"[^\d,.\-]", "", s)
    if "," in s and "." in s: s = s.replace(",", "")
    elif "," in s:           s = s.replace(",", ".")
    try: return float(s)
    except: return None

def gmt_suffix_for(dt_local: datetime) -> str:
    if not dt_local or dt_local.utcoffset() is None: return "GMT"
    hours = int(dt_local.utcoffset().total_seconds() // 3600)
    return "GMT" if hours == 0 else f"GMT{('+' if hours > 0 else '-')}{abs(hours)}"

def format_date_field(dt_local: datetime, ccy: str, exch_label: str, source: str) -> str:
    if not dt_local: return f" -  | {ccy} | {exch_label} | Source: {source}"
    month_str = dt_local.strftime("%b")
    day_str = str(int(dt_local.strftime("%d")))
    time_str = dt_local.strftime("%I:%M:%S %p").lstrip("0")
    return f"{month_str} {day_str}, {time_str} {gmt_suffix_for(dt_local)} | {ccy} | {exch_label} | Source: {source}"

def std_cols(df: pd.DataFrame, source: str) -> pd.DataFrame:
    df = df.copy(); colmap = {c.lower(): c for c in df.columns}
    def pick(*names):
        for n in names:
            if n in colmap: return colmap[n]
        return None
    stock_col = pick("stock:exch","stock_exch","stock","symbol","ticker_exch")
    tick_col  = pick("ticker","mnemonic","symbol_only")
    price_col = pick("price","last","last_price","last traded")
    date_col  = pick("date","time","updated","as of")
    out = pd.DataFrame()
    out["STOCK:EXCH"] = df[stock_col] if stock_col else ""
    out["TICKER"]     = df[tick_col] if tick_col else ""
    out["PRICE"]      = pd.to_numeric(df[price_col], errors="coerce") if price_col else np.nan
    out["DATE"]       = df[date_col] if date_col else ""
    out["SOURCE"]     = source
    out["STOCK:EXCH"] = out["STOCK:EXCH"].astype(str).str.upper().str.strip()
    out["TICKER"]     = out["TICKER"].astype(str).str.upper().str.strip()
    return out[["SOURCE","STOCK:EXCH","TICKER","PRICE","DATE"]]

def split_date_meta(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["EXCHANGE"] = df["STOCK:EXCH"].astype(str).str.split(":", n=1).str[1].fillna("").str.strip()
    raw = df["DATE"].astype(str).str.replace("-", "|").apply(norm_ws)

    def extract_parts(s: str):
        if not s: return "", "", ""
        parts = [p.strip() for p in s.split("|") if p.strip()]
        date_text, currency, source = (parts[0] if parts else ""), "", ""
        for p in parts[1:]:
            if p.lower().startswith("source"):
                m = re.search(r"source\s*:\s*(.+)", p, flags=re.I)
                source = (m.group(1).strip() if m else p.replace("Source:", "").strip())
            elif re.fullmatch(r"[A-Z]{2,4}", p):
                currency = p
        return date_text, currency, source

    split = raw.apply(extract_parts)
    df["DATE"]     = split.apply(lambda x: x[0])
    df["CURRENCY"] = split.apply(lambda x: x[1])
    src_from_date  = split.apply(lambda x: x[2])
    df["SOURCE"]   = np.where(src_from_date.astype(str).str.len() > 0, src_from_date, df.get("SOURCE",""))
    df["SOURCE"]   = df["SOURCE"].astype(str).str.replace(r"^\s*Source\s*:\s*", "", regex=True).str.strip()
    return df

def _normalize_ticker_key(value: str) -> str:
    return (value or "").strip().upper()

def fetch_yahoo_previous_closes(symbols: List[str]) -> tuple[Dict[str, float], List[str]]:
    unique = sorted({(s or "").strip() for s in symbols if (s or "").strip()})
    out: Dict[str, float] = {}
    warnings: List[str] = []
    if not unique:
        return out, warnings
    try:
        import yfinance as yf
    except Exception as exc:
        warnings.append(f"Yahoo Finance unavailable: {exc}")
        return out, warnings

    def prev_close_from_history(hist: pd.DataFrame) -> float | None:
        if hist is None or hist.empty or "Close" not in hist:
            return None
        closes = hist["Close"].dropna()
        if len(closes) >= 2:
            try:
                return float(closes.iloc[-2])
            except Exception:
                return None
        if len(closes) == 1:
            try:
                return float(closes.iloc[-1])
            except Exception:
                return None
        return None

    for sym in unique:
        val = None
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="5d", interval="1d", auto_adjust=False)
            val = prev_close_from_history(hist)
            if val is None:
                fi = getattr(ticker, "fast_info", None)
                if isinstance(fi, dict):
                    for key in ("previous_close", "previousClose", "last_price", "lastPrice"):
                        if fi.get(key) is not None:
                            try:
                                val = float(fi[key])
                                break
                            except Exception:
                                continue
        except Exception as exc:
            warnings.append(f"Yahoo lookup failed for {sym}: {exc}")
            val = None

        if val is not None:
            out[sym] = val
        else:
            warnings.append(f"Yahoo previous close missing for {sym}")

    return out, warnings

def attach_yahoo_change(equities_df: pd.DataFrame) -> tuple[pd.DataFrame, List[str]]:
    if equities_df is None or equities_df.empty or not YAHOO_TICKERS:
        equities_df = equities_df.copy()
        equities_df["Y_PREV_CLOSE"] = np.nan
        return equities_df, []

    ticker_map: Dict[int, str] = {}
    needed: List[str] = []
    for idx, t in equities_df["TICKER"].items():
        key = _normalize_ticker_key(t)
        yahoo = YAHOO_TICKERS.get(key)
        if yahoo:
            ticker_map[idx] = yahoo
            needed.append(yahoo)

    prev_map, warnings = fetch_yahoo_previous_closes(needed)
    prev_values = []
    for idx in range(len(equities_df)):
        yahoo = ticker_map.get(idx)
        prev = prev_map.get(yahoo) if yahoo else None
        try:
            prev_values.append(float(prev))
        except (TypeError, ValueError):
            prev_values.append(np.nan)

    equities_df = equities_df.copy()
    equities_df["Y_PREV_CLOSE"] = prev_values
    return equities_df, warnings


def attach_forex_change(forex_df: pd.DataFrame) -> tuple[pd.DataFrame, List[str]]:
    if forex_df is None or forex_df.empty:
        forex_df = forex_df.copy() if forex_df is not None else pd.DataFrame()
        if "CHANGE" not in forex_df.columns:
            forex_df["CHANGE"] = np.nan
        return forex_df, []

    ticker_map: Dict[int, str] = {}
    needed: List[str] = []
    for idx, pair in forex_df["TICKER"].items():
        raw = (str(pair) or "").upper().strip()
        if not raw:
            continue
        yahoo = raw.replace("/", "") + "=X"
        ticker_map[idx] = yahoo
        needed.append(yahoo)

    prev_map, warnings = fetch_yahoo_previous_closes(needed)
    prev_values = []
    for idx in range(len(forex_df)):
        yahoo = ticker_map.get(idx)
        prev = prev_map.get(yahoo) if yahoo else None
        try:
            prev_values.append(float(prev))
        except (TypeError, ValueError):
            prev_values.append(np.nan)

    forex_df = forex_df.copy()
    forex_df["Y_PREV_CLOSE"] = prev_values
    price = pd.to_numeric(forex_df.get("PRICE"), errors="coerce")
    prev = pd.to_numeric(forex_df["Y_PREV_CLOSE"], errors="coerce")
    with np.errstate(divide="ignore", invalid="ignore"):
        pct = (price - prev) / prev * 100.0
    forex_df["CHANGE"] = pct
    forex_df.drop(columns=["Y_PREV_CLOSE"], inplace=True, errors="ignore")
    return forex_df, warnings

# ================== CONFIG LOADER ==================
def load_config_from_local_csv(path: str):
    import os, io, csv
    import pandas as pd

    if not path or not os.path.isfile(path):
        raise RuntimeError(f"Config file not found at: {path}")

    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, dtype=str)
    else:
        with open(path, "rb") as f:
            raw = f.read()
        if raw.startswith(b"\xef\xbb\xbf"): raw = raw[3:]
        text = raw.decode("utf-8", errors="replace")
        try:
            lines = text.splitlines(); probe = "\n".join(lines[:5])
            dialect = csv.Sniffer().sniff(probe); sep = dialect.delimiter
        except Exception:
            sep = None
        df = pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False, sep=sep)

    if df is None or df.empty:
        raise RuntimeError("Config file is empty after parsing")

    df.columns = [str(c).strip().lower() for c in df.columns]
    if "zb_exch" in df.columns and "zb_exchange" not in df.columns:
        df.rename(columns={"zb_exch": "zb_exchange"}, inplace=True)
    for need in ["source","symbol","url","ticker","zb_exchange"]:
        if need not in df.columns: df[need] = ""
    for c in df.columns:
        if df[c].dtype == object: df[c] = df[c].astype(str).str.strip()

    def norm_source(s: str) -> str:
        s = (s or "").strip().lower()
        if s in ("google", "google finance", "gfinance", "gfin"): return "google"
        if s in ("euronext", "enext"): return "euronext"
        if s in ("zonebourse", "zone bourse", "zb"): return "zonebourse"
        if s in ("fx", "forex", "fx_tv", "tradingview_fx", "tv_fx"): return "fx"
        return s
    df["source"] = df["source"].apply(norm_source)

    # GOOGLE
    gmask = df["source"].eq("google") & df["symbol"].str.len().gt(0)
    symbols = df.loc[gmask, "symbol"].str.upper().tolist()
    seen = set(); google_syms = []
    for s in symbols:
        if s not in seen:
            seen.add(s); google_syms.append(s)
    globals()["GOOGLE_SYMBOLS"] = google_syms

    # EURONEXT
    emask = df["source"].eq("euronext") & df["url"].str.len().gt(0)
    e_list = []
    for _, r in df.loc[emask, ["url","ticker"]].iterrows():
        e_list.append({"url": r["url"], "ticker": (r.get("ticker","") or "").upper()})
    globals()["EURONEXT_BATCH"] = e_list

    # ZONE BOURSE
    zmask = df["source"].eq("zonebourse") & df["url"].str.len().gt(0)
    z_list = []; zb_exch = {}
    for _, r in df.loc[zmask, ["url","ticker","zb_exchange"]].iterrows():
        tkr = (r.get("ticker","") or "").upper()
        if tkr:
            z_list.append((tkr, r["url"]))
            exo = (r.get("zb_exchange","") or "").upper()
            if exo: zb_exch[tkr] = exo
    base_map = {
        "TATE": "LON", "YCA": "LON", "BP": "LON", "AAL": "LON", "LSEG": "LON",
        "META": "NASDAQ", "WY": "NYSE", "EBRO": "BMI", "SWON": "SIX", "LAND": "SIX",
    }
    base_map.update(zb_exch)
    globals()["ZB_STOCKS"] = z_list
    globals()["ZB_EXCHANGES"] = base_map

    # FX via TradingView
    fmask = df["source"].eq("fx") & df["symbol"].str.len().gt(0)
    fx_pairs = (
        df.loc[fmask, "symbol"]
        .str.upper()
        .str.replace(r"[^A-Z0-9]", "", regex=True)
        .tolist()
    )
    globals()["FX_PAIRS"] = fx_pairs

    yahoo_col = None
    for cand in ("yahoo_ticker", "yahoo ticker", "yahoo"):
        if cand in df.columns:
            yahoo_col = cand
            break
    yahoo_map: Dict[str, str] = {}
    if yahoo_col:
        for _, r in df.iterrows():
            base = (r.get("ticker", "") or "").upper().strip()
            alt = (r.get("symbol", "") or "").upper().strip()
            yahoo = (r.get(yahoo_col, "") or "").strip()
            key = base or alt
            if key and yahoo:
                yahoo_map[key] = yahoo
    globals()["YAHOO_TICKERS"] = yahoo_map

    print(f"[CONFIG] Loaded -> Google={len(GOOGLE_SYMBOLS)} | Euronext={len(EURONEXT_BATCH)} | ZoneBourse={len(ZB_STOCKS)} | FX={len(FX_PAIRS)} | Yahoo={len(YAHOO_TICKERS)}")
    try: print(df.head(8).to_string(index=False))
    except Exception: pass

# ================== GOOGLE FINANCE ==================
PRICE_SELECTORS = ["div.YMlKec.fxKbKc", "div.YMlKec", '[data-last-price]']
TIME_SELECTORS  = ['[jsname="Vebqub"]', '[data-last-normal-market-timestamp]', 'div[data-source="google-finance"] time']

def massage_google_time(raw: str) -> str:
    s = norm_ws(raw or "").replace("-","|")
    s = re.sub(r"^(As of|Updated)\s*", "", s, flags=re.I)
    if "Source:" not in s:
        s = f"{s} | Source: Google Finance" if s else " -  | Source: Google Finance"
    return s.strip(" |")

async def fetch_google(ctx, symbols: List[str], concurrency: int = 8):
    if not symbols:
        return pd.DataFrame(columns=["STOCK:EXCH","TICKER","PRICE","DATE"]), [], 0.0
    t0 = time.perf_counter()
    sem = asyncio.Semaphore(max(1, concurrency))
    results: Dict[str, Dict[str, Any]] = {}

    async def route_handler(route):
        rtype = route.request.resource_type
        if "google.com/finance" in route.request.url and rtype in {"image","media","font","stylesheet"}:
            return await route.abort()
        return await route.continue_()
    await ctx.route("**/*", route_handler)

    warm = await ctx.new_page()
    try:
        await warm.goto("https://www.google.com/finance?hl=en", timeout=20000)
        try:
            consent_frame = next((fr for fr in warm.frames if "consent.google.com" in (fr.url or "").lower()), None)
            if consent_frame:
                for label in [r"Reject all", r"Accept all", r"I agree", r"Tout refuser", r"Tout accepter"]:
                    try:
                        await consent_frame.get_by_role("button", name=re.compile(label, re.I)).click(timeout=2000)
                        await warm.wait_for_load_state("networkidle", timeout=8000)
                        break
                    except: continue
        except: pass
    finally:
        await warm.close()

    per_times: List[Tuple[str, float]] = []

    async def one(sym: str):
        url = f"https://www.google.com/finance/quote/{sym}?hl=en"
        p0 = time.perf_counter()
        page = await ctx.new_page()
        try:
            await page.goto(url, timeout=20000)
            price_val = None
            for sel in PRICE_SELECTORS:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        raw = await (el.get_attribute("data-last-price") if sel == '[data-last-price]' else el.inner_text())
                        f = to_float_any(raw)
                        if f is not None:
                            price_val = f; break
                except: continue
            time_txt = None
            for sel in TIME_SELECTORS:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        t = await el.inner_text()
                        if t and t.strip(): time_txt = t.strip(); break
                        v = await el.get_attribute("datetime") or await el.get_attribute("data-last-normal-market-timestamp")
                        if v: time_txt = v; break
                except: continue
            results[sym] = {"STOCK:EXCH": sym.upper(),
                            "TICKER": sym.upper().split(":",1)[0],
                            "PRICE": price_val,
                            "DATE": massage_google_time(time_txt)}
        except:
            results[sym] = {"STOCK:EXCH": sym.upper(),
                            "TICKER": sym.upper().split(":",1)[0],
                            "PRICE": None,
                            "DATE": " -  | Source: Google Finance"}
        finally:
            per_times.append((sym, time.perf_counter() - p0))
            await page.close()

    async def guard(sym: str):
        async with sem:
            await one(sym)

    await asyncio.gather(*(guard(s) for s in symbols))
    df = pd.DataFrame(list(results.values()), columns=["STOCK:EXCH","TICKER","PRICE","DATE"])
    elapsed = time.perf_counter() - t0
    return df, per_times, elapsed

# ================== EURONEXT ==================
def _httpx_and_http2_flag():
    try:
        import httpx  # noqa
    except Exception:
        return None, False
    http2_ok = importlib.util.find_spec("h2") is not None
    return importlib.import_module("httpx"), http2_ok

MIC_TO_DISPLAY = {
    "XBRU": "BRU","XPAR": "PAR","XAMS": "AMS","XLIS": "LIS",
    "XMAD": "MAD","XDUB": "DUB","XMSM": "DUB","XMIL": "MIL","MTAA": "BIT",
}
MIC_TO_TZ = {
    "XBRU": "Europe/Brussels","XPAR": "Europe/Paris","XAMS": "Europe/Amsterdam","XLIS": "Europe/Lisbon",
    "XMAD": "Europe/Madrid","XDUB": "Europe/Dublin","XMSM": "Europe/Dublin","XMIL": "Europe/Rome","MTAA": "Europe/Rome",
}

def display_exchange_from_mic(mic: str) -> str:
    return MIC_TO_DISPLAY.get((mic or "").upper(), (mic or "").upper())

def local_tz_from_mic(mic: str) -> str:
    return MIC_TO_TZ.get((mic or "").upper(), "Europe/Paris")

def parse_local_timestamp(s: str, local_tz="Europe/Paris"):
    s = (s or "").strip().strip("[]").strip()
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=ZoneInfo(local_tz))
        except:
            continue
    try:
        from dateutil import parser as dateparser
        return dateparser.isoparse(s).astimezone(ZoneInfo(local_tz))
    except:
        return None

def to_float_eu(txt: str):
    t = (txt or "").strip().replace("\xa0"," ").replace(" ", "")
    if "," in t and "." in t:
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "").replace(",", ".")
        else:
            t = t.replace(",", "")
    else:
        if "," in t: t = t.replace(",", ".")
    try: return float(t)
    except: return None

def extract_isin_mic_from_url(url: str):
    m = re.search(r"/product/equities/([A-Z0-9]+)-([A-Z0-9]+)", url or "", re.I)
    if not m: return None, None
    return m.group(1).upper(), m.group(2).upper()

async def fetch_euronext_httpx(entries, max_concurrency: int = 10, retries: int = 3, timeout_s: int = 12):
    httpx, http2_ok = _httpx_and_http2_flag()
    if httpx is None: raise RuntimeError("httpx is not installed")
    t0 = time.perf_counter()
    sem = asyncio.Semaphore(max(1, max_concurrency))
    rows: List[Dict[str, Any]] = []
    per_times: List[Tuple[str, float]] = []

    print(f"Euronext HTTP mode: {'HTTP/2' if http2_ok else 'HTTP/1.1'}")

    async with httpx.AsyncClient(http2=http2_ok, headers=HEADERS, timeout=timeout_s) as client:
        async def one(entry):
            url = entry["url"] if isinstance(entry, dict) else entry[0]
            given_ticker = (entry.get("ticker") if isinstance(entry, dict) else (entry[1] if len(entry) > 1 else "")) or ""
            p0 = time.perf_counter()
            try:
                isin, mic = extract_isin_mic_from_url(url or "")
                exch_disp = display_exchange_from_mic(mic or "")
                for attempt in range(1, retries+1):
                    try:
                        ajax = f"https://live.euronext.com/en/intraday_chart/getDetailedQuoteAjax/{isin}-{mic}/full"
                        r = await client.get(ajax, headers={
                            "Referer": f"https://live.euronext.com/en/product/equities/{isin}-{mic}",
                            "X-Requested-With": "XMLHttpRequest",
                        })
                        r.raise_for_status()
                        soup = BeautifulSoup(r.text, "html.parser")
                        label_map = {}
                        for tr in soup.find_all("tr"):
                            tds = tr.find_all(["td","th"])
                            if len(tds) >= 2:
                                label = tds[0].get_text(strip=True).lower()
                                vals  = [td.get_text(strip=True) for td in tds[1:]]
                                label_map[label] = vals
                        ticker = given_ticker.upper() or next((label_map[k][0].strip().upper()
                                                              for k in ("mnemonic","symbol","ticker","trading symbol","code")
                                                              if k in label_map and label_map[k]), "")
                        ccy = next((label_map[k][0].strip().upper()
                                    for k in ("currency","curr.","ccy")
                                    if k in label_map and label_map[k]), "EUR")
                        local_tz = local_tz_from_mic(mic)
                        price = None
                        dt_local = None
                        for k in ("last traded","last price","last","price","last traded price"):
                            if k in label_map and label_map[k]:
                                price = to_float_eu(label_map[k][0])
                                if len(label_map[k]) >= 2 and not dt_local:
                                    dt_local = parse_local_timestamp(label_map[k][1], local_tz=local_tz)
                                if price is not None: break
                        if not dt_local:
                            for k in ("last traded","last update","last trading time","time"):
                                if k in label_map:
                                    for v in label_map[k]:
                                        dt_local = parse_local_timestamp(v, local_tz=local_tz)
                                        if dt_local: break
                                if dt_local: break
                        date_field = format_date_field(dt_local, ccy, exch_disp, source="Euronext")
                        rows.append({"STOCK:EXCH": f"{ticker}:{exch_disp}" if ticker else f":{exch_disp}",
                                     "TICKER": ticker,"PRICE": price,"DATE": date_field})
                        per_times.append((ticker or exch_disp, time.perf_counter() - p0)); return
                    except Exception:
                        await asyncio.sleep((0.25 * (2 ** (attempt-1))) + random.uniform(0, 0.2))
                rows.append({"STOCK:EXCH": f"{given_ticker.upper()}:{exch_disp}",
                             "TICKER": given_ticker.upper(),"PRICE": None,"DATE": " -  | Source: Euronext"})
                per_times.append((given_ticker or " - ", time.perf_counter() - p0))
            except Exception:
                rows.append({"STOCK:EXCH":"", "TICKER":"", "PRICE":None, "DATE":" -  | Source: Euronext"})
                per_times.append((" - ", time.perf_counter() - p0))

        async def guard(entry):
            async with sem:
                await one(entry)

        await asyncio.gather(*(guard(e) for e in entries))

    df = pd.DataFrame(rows, columns=["STOCK:EXCH","TICKER","PRICE","DATE"])
    elapsed = time.perf_counter() - t0
    return df, per_times, elapsed

def fetch_euronext_requests(entries, max_workers: int = 8, retries: int = 3, timeout_s: int = 12):
    t0 = time.perf_counter()
    rows: List[Dict[str, Any]] = []
    per_times: List[Tuple[str, float]] = []

    def worker(entry):
        url = entry["url"] if isinstance(entry, dict) else entry[0]
        given_ticker = (entry.get("ticker") if isinstance(entry, dict) else (entry[1] if len(entry) > 1 else "")) or ""
        p0 = time.perf_counter()
        try:
            isin, mic = extract_isin_mic_from_url(url or "")
            exch_disp = display_exchange_from_mic(mic or "")
            for attempt in range(1, retries+1):
                try:
                    ajax = f"https://live.euronext.com/en/intraday_chart/getDetailedQuoteAjax/{isin}-{mic}/full"
                    r = requests.get(ajax, headers={
                        "User-Agent": HEADERS["User-Agent"],
                        "Referer": f"https://live.euronext.com/en/product/equities/{isin}-{mic}",
                        "X-Requested-With": "XMLHttpRequest",
                        "Accept-Language": "en-US,en;q=0.9",
                    }, timeout=timeout_s)
                    r.raise_for_status()
                    soup = BeautifulSoup(r.text, "html.parser")
                    label_map = {}
                    for tr in soup.find_all("tr"):
                        tds = tr.find_all(["td","th"])
                        if len(tds) >= 2:
                            label = tds[0].get_text(strip=True).lower()
                            vals  = [td.get_text(strip=True) for td in tds[1:]]
                            label_map[label] = vals
                    ticker = given_ticker.upper() or next((label_map[k][0].strip().upper()
                                                          for k in ("mnemonic","symbol","ticker","trading symbol","code")
                                                          if k in label_map and label_map[k]), "")
                    ccy = next((label_map[k][0].strip().upper()
                                for k in ("currency","curr.","ccy")
                                if k in label_map and label_map[k]), "EUR")
                    local_tz = local_tz_from_mic(mic)
                    price = None
                    dt_local = None
                    for k in ("last traded","last price","last","price","last traded price"):
                        if k in label_map and label_map[k]:
                            price = to_float_eu(label_map[k][0])
                            if len(label_map[k]) >= 2 and not dt_local:
                                dt_local = parse_local_timestamp(label_map[k][1], local_tz=local_tz)
                            if price is not None: break
                    if not dt_local:
                        for k in ("last traded","last update","last trading time","time"):
                            if k in label_map:
                                for v in label_map[k]:
                                    dt_local = parse_local_timestamp(v, local_tz=local_tz)
                                    if dt_local: break
                            if dt_local: break
                    date_field = format_date_field(dt_local, ccy, exch_disp, source="Euronext")
                    rows.append({"STOCK:EXCH": f"{ticker}:{exch_disp}" if ticker else f":{exch_disp}",
                                 "TICKER": ticker,"PRICE": price,"DATE": date_field})
                    per_times.append((ticker or exch_disp, time.perf_counter() - p0)); return
                except Exception:
                    time.sleep((0.25 * (2 ** (attempt-1))) + random.uniform(0, 0.2))
            rows.append({"STOCK:EXCH": f"{given_ticker.upper()}:{exch_disp}",
                         "TICKER": given_ticker.upper(),"PRICE": None,"DATE": " -  | Source: Euronext"})
            per_times.append((given_ticker or " - ", time.perf_counter() - p0))
        except Exception:
            rows.append({"STOCK:EXCH":"", "TICKER":"", "PRICE":None, "DATE":" -  | Source: Euronext"})
            per_times.append((" - ", time.perf_counter() - p0))

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as ex:
        futs = [ex.submit(worker, e) for e in entries]
        for _ in as_completed(futs): pass

    df = pd.DataFrame(rows, columns=["STOCK:EXCH","TICKER","PRICE","DATE"])
    elapsed = time.perf_counter() - t0
    print("Euronext HTTP mode: HTTP/1.1 (requests fallback)")
    return df, per_times, elapsed

# ================== ZONE BOURSE ==================
ZB_PRICE_SELECTORS = [
    {'name': 'span', 'attrs': {'itemprop': 'price'}},
    {'name': 'span', 'attrs': {'class': re.compile(r'(c-instrument__last|price|last|cours-last)', re.I)}},
    {'name': 'div',  'attrs': {'class': re.compile(r'(price|last|cours-last)', re.I)}},
]

def _zb_extract_price_currency(soup: BeautifulSoup) -> tuple[float | None, str | None]:
    tag = soup.find('span', attrs={'itemprop': 'price'})
    if tag:
        raw = tag.get('content') or tag.get_text(strip=True)
        val = to_float_any(raw)
        cur = None
        cur_tag = soup.find(attrs={'itemprop': 'priceCurrency'})
        if cur_tag:
            cur = (cur_tag.get('content') or cur_tag.get_text(strip=True) or '').upper()
        if val is not None:
            return val, cur
    for s in soup.find_all('script'):
        txt = s.string or s.get_text()
        if not txt: continue
        m = re.search(r'"(?:price|last|lastPrice)"\s*:\s*([0-9]+(?:[.,][0-9]+)?)', txt)
        if m:
            val = to_float_any(m.group(1))
            m2 = re.search(r'"(?:currency|ccy)"\s*:\s*"([A-Za-z]{2,4})"', txt)
            cur = (m2.group(1).upper() if m2 else None)
            if val is not None:
                return val, cur
    for sel in ZB_PRICE_SELECTORS:
        tag = soup.find(sel['name'], attrs=sel['attrs'])
        if tag:
            val = to_float_any(tag.get_text(" ", strip=True))
            if val is not None:
                cur = None
                parent = tag.parent
                if parent:
                    near = parent.get_text(" ", strip=True)
                    mcur = re.search(r'\b(CHF|EUR|USD|GBX|GBP)\b', near, re.I)
                    if mcur:
                        cur = mcur.group(1).upper()
                return val, cur
    return None, None

def _zb_format_date_now(exch: str, currency: str | None) -> str:
    ts = datetime.now(PARIS)
    ccy = (currency or EXCHANGE_TO_CURRENCY.get(exch, "")).upper()
    return f"{ts.strftime('%b %d, %I:%M:%S %p').lstrip('0')} {gmt_suffix_for(ts)} | {ccy} | {exch} | Source: Zone Bourse"

async def fetch_zonebourse_http(entries, max_concurrency: int = 8, timeout_s: int = 10):
    httpx, _http2 = _httpx_and_http2_flag()
    if httpx is None:
        raise RuntimeError("httpx is not installed")
    t0 = time.perf_counter()
    sem = asyncio.Semaphore(max(1, max_concurrency))
    rows: list[dict[str, Any]] = []
    per_times: list[tuple[str, float]] = []

    async with httpx.AsyncClient(http2=True, headers=HEADERS, timeout=timeout_s, follow_redirects=True) as client:
        async def one(ticker: str, url: str):
            p0 = time.perf_counter()
            exch = ZB_EXCHANGES.get(ticker.upper(), " - ")
            try:
                r = await client.get(url, headers={"Referer": "https://www.zonebourse.com/"})
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
                price, cur = _zb_extract_price_currency(soup)
                rows.append({
                    "STOCK:EXCH": f"{ticker}:{exch}",
                    "TICKER": ticker.upper(),
                    "PRICE": float(price) if price is not None else None,
                    "DATE": _zb_format_date_now(exch, cur),
                })
            except Exception:
                rows.append({
                    "STOCK:EXCH": f"{ticker}:{exch}",
                    "TICKER": ticker.upper(),
                    "PRICE": None,
                    "DATE": f" -  | {EXCHANGE_TO_CURRENCY.get(exch,'')} | {exch} | Source: Zone Bourse",
                })
            finally:
                per_times.append((ticker, time.perf_counter() - p0))

        async def guard(t, u):
            async with sem:
                await one(t, u)

        await asyncio.gather(*(guard(t, u) for t, u in entries))

    df = pd.DataFrame(rows, columns=["STOCK:EXCH","TICKER","PRICE","DATE"])
    elapsed = time.perf_counter() - t0
    return df, per_times, elapsed

async def fetch_zonebourse_playwright_dom(ctx, entries, timeout_ms: int = 8000):
    out: dict[str, tuple[float | None, str | None]] = {}

    async def read_one(ticker: str, url: str):
        page = await ctx.new_page()
        try:
            await page.route("**/*", lambda route: (
                asyncio.create_task(route.abort()) if route.request.resource_type in {"image","media","font","stylesheet"} else asyncio.create_task(route.continue_())
            ))
            await page.goto(url, timeout=timeout_ms)
            txt = None
            for sel in ["span[itemprop='price']","span.c-instrument__last","span.price","div.price","span[class*='last']"]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        raw = (await el.get_attribute("content")) or (await el.inner_text())
                        if raw and raw.strip():
                            txt = raw.strip(); break
                except: continue
            price = to_float_any(txt) if txt else None

            cur = None
            try:
                elc = await page.query_selector("meta[itemprop='priceCurrency']")
                if elc:
                    cur = (await elc.get_attribute("content")) or (await elc.inner_text())
                    if cur: cur = cur.strip().upper()
            except: pass

            out[ticker.upper()] = (price, cur)
        except:
            out[ticker.upper()] = (None, None)
        finally:
            await page.close()

    await asyncio.gather(*(read_one(t, u) for t, u in entries))
    return out

# ================== TRADINGVIEW FX ==================
TV_SCANNER_URL = "https://scanner.tradingview.com/forex/scan"
TV_HEADERS = {
    "User-Agent": HEADERS["User-Agent"],
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://www.tradingview.com",
    "Referer": "https://www.tradingview.com/",
}

def _fx_quote_ccy(pair: str) -> str:
    p = (pair or "").upper()
    return p[3:6] if len(p) >= 6 else ""

def _fx_format_local_datetime_like_sample(dt_utc: datetime) -> str:
    local = dt_utc.astimezone()
    offset_minutes = int(local.utcoffset().total_seconds() // 60)
    sign = "+" if offset_minutes >= 0 else "-"
    hours = abs(offset_minutes) // 60
    return local.strftime(f"%b %d, %I:%M:%S %p GMT{sign}{hours}")

def fetch_fx_tradingview(pairs: List[str]) -> tuple[pd.DataFrame, list[tuple[str,float]], float]:
    """Return df with SOURCE, STOCK:EXCH, TICKER, PRICE (unmodified), DATE (timestamp only), EXCHANGE, CURRENCY."""
    if not pairs:
        empty = pd.DataFrame(columns=["SOURCE","STOCK:EXCH","TICKER","PRICE","DATE","EXCHANGE","CURRENCY"])
        return empty, [], 0.0

    t0 = time.perf_counter()

    tickers = [f"FX_IDC:{p.upper()}" for p in pairs]
    body = {"symbols": {"tickers": tickers, "query": {"types": []}}, "columns": ["close", "name"]}

    per_times = []
    try:
        p0 = time.perf_counter()
        resp = requests.post(TV_SCANNER_URL, headers=TV_HEADERS, data=json.dumps(body), timeout=8)
        resp.raise_for_status()
        payload = resp.json()
        per_times.append(("TradingView scan", time.perf_counter() - p0))
    except Exception:
        payload = {"data": []}

    quotes: Dict[str, float] = {}
    for row in payload.get("data", []):
        s = row.get("s", "")
        d = row.get("d", [])
        if not d: continue
        px = d[0]
        base = s.split(":", 1)[1] if ":" in s else s   # e.g., "EURUSD"
        if isinstance(px, (int, float)):
            quotes[base] = float(px)

    # DATE = timestamp only
    now_utc = datetime.now(timezone.utc)
    date_only = _fx_format_local_datetime_like_sample(now_utc)

    rows = []
    for pair in pairs:
        pp = pair.upper()
        price = quotes.get(pp, float("nan"))  # IMPORTANT: leave value as-is (no scaling/rounding)
        rows.append({
            "SOURCE": "TradingView",
            "STOCK:EXCH": f"{pp}:FX_IDC",
            "TICKER": pp,
            "PRICE": price,
            "DATE": date_only,
            "EXCHANGE": "FX_IDC",
            "CURRENCY": _fx_quote_ccy(pp),
        })

    df = pd.DataFrame(rows, columns=["SOURCE","STOCK:EXCH","TICKER","PRICE","DATE","EXCHANGE","CURRENCY"])
    total = time.perf_counter() - t0
    return df, per_times, total

# ================== EXCEL WRITER ==================
def _excel_find_open_wb(xl, target: str):
    target_low = target.lower()
    for wb in xl.Workbooks:
        try:
            if wb.FullName.lower() == target_low or wb.Name.lower() == os.path.basename(target_low):
                return wb
        except Exception: continue
    name_only = os.path.basename(target_low)
    for wb in xl.Workbooks:
        try:
            if wb.Name.lower() == name_only: return wb
        except Exception: continue
    return None

def _write_table(ws, start_row: int, df: pd.DataFrame, title: str | None = None) -> int:
    """
    Writes an optional title, then df header + values.
    Returns the next empty row index after the table.
    """
    r = start_row
    if title:
        ws.Cells(r, 1).Value2 = title
        ws.Cells(r, 1).Font.Bold = True
        r += 1

    if df is None or df.empty:
        ws.Cells(r, 1).Value2 = "(no data)"
        r += 1
        return r

    headers = list(df.columns)
    df2 = df.copy()
    if "PRICE" in df2.columns:
        df2["PRICE"] = pd.to_numeric(df2["PRICE"], errors="coerce").astype(float)

    total_rows = len(df2) + 1
    total_cols = len(headers)

    ws.Range(ws.Cells(r, 1), ws.Cells(r, total_cols)).Value2 = [headers]
    ws.Rows(r).Font.Bold = True

    if len(df2) > 0:
        rng = ws.Range(ws.Cells(r + 1, 1), ws.Cells(r + len(df2), total_cols))
        rng.Value2 = df2.astype(object).values.tolist()

        # Default PRICE number format (equities)  -  2 dp
        try:
            if "PRICE" in headers:
                price_idx = headers.index("PRICE") + 1
                rng_price = ws.Range(ws.Cells(r + 1, price_idx), ws.Cells(r + len(df2), price_idx))
                try: rng_price.ClearFormats()
                except: pass
                try: rng_price.FormatConditions.Delete()
                except: pass
                set_ok = False
                try:
                    rng_price.NumberFormatLocal = "0,00"
                    set_ok = True
                except: pass
                if not set_ok:
                    try:
                        rng_price.NumberFormat = "0.00"
                        set_ok = True
                    except: pass
                if not set_ok:
                    dec = getattr(ws.Parent, "DecimalSeparator", ".")
                    rng_price.NumberFormatLocal = f"0{dec}00"
        except Exception:
            pass

    ws.Columns.AutoFit()
    return r + total_rows

def _apply_price_decimal_format(ws, header_row: int, n_rows: int, headers: list[str], decimals: int):
    """Apply Excel number format to the PRICE column with `decimals` decimals (values untouched)."""
    if n_rows <= 0 or not headers: return
    upper = [h.upper() for h in headers]
    if "PRICE" not in upper: return
    price_col_idx = upper.index("PRICE") + 1
    first_data_row = header_row + 1
    last_data_row  = header_row + n_rows
    rng_price = ws.Range(ws.Cells(first_data_row, price_col_idx),
                         ws.Cells(last_data_row,  price_col_idx))
    try:
        try: rng_price.ClearFormats()
        except: pass
        try: rng_price.FormatConditions.Delete()
        except: pass
        # Prefer local pattern (e.g. FR: "0,0000")
        try:
            rng_price.NumberFormatLocal = "0," + ("0" * decimals)
            return
        except: pass
        # Fallback invariant
        try:
            rng_price.NumberFormat = "0." + ("0" * decimals)
            return
        except: pass
        # Last resort using app decimal separator
        try:
            dec = getattr(ws.Parent, "DecimalSeparator", ".")
            rng_price.NumberFormatLocal = "0" + (dec + ("0" * decimals) if decimals > 0 else "")
        except: pass
    except:
        pass

def write_two_tables_to_excel_new_sheet(target_workbook: str,
                                        equities_df: pd.DataFrame,
                                        forex_df: pd.DataFrame,
                                        timings: Dict[str, Any]):
    if not HAS_COM or pythoncom is None or win32 is None:
        raise RuntimeError("Excel COM automation is unavailable in this environment (pywin32 missing).")
    pythoncom.CoInitialize()
    xl = None
    opened_here = False
    try:
        try:
            xl = win32.GetObject(Class="Excel.Application")
        except Exception:
            xl = win32.Dispatch("Excel.Application")
        xl.Visible = True

        wb = _excel_find_open_wb(xl, target_workbook)
        if wb is None and os.path.isfile(target_workbook):
            wb = xl.Workbooks.Open(target_workbook)
            opened_here = True
        if wb is None:
            raise RuntimeError(f"Workbook not found or not open: {target_workbook}")

        sheet_name = "LFPI_" + datetime.now(PARIS).strftime("%Y%m%d_%H%M%S")
        ws = wb.Worksheets.Add(After=wb.Worksheets(wb.Worksheets.Count))
        try:
            ws.Name = sheet_name
        except Exception:
            pass

        # clear base formatting
        try: ws.Cells.ClearFormats()
        except Exception: pass
        try: ws.Cells.FormatConditions.Delete()
        except Exception: pass
        try: ws.Cells.Style = "Normal"
        except Exception: pass
        for fmt in ("Standard", "General"):
            try:
                ws.Cells.NumberFormatLocal = fmt
                break
            except Exception:
                continue

        # 1) write equities table (2 dp via _write_table)
        next_row = _write_table(ws, 1, equities_df, title=None)

        # 2) blank line
        next_row += 1

        # 3) PREP FX: ensure numeric ONLY (no scaling, no rounding)
        fx_df_to_write = forex_df.copy()
        if "PRICE" in fx_df_to_write.columns:
            fx_df_to_write["PRICE"] = pd.to_numeric(fx_df_to_write["PRICE"], errors="coerce")

        # 4) write FOREX table
        fx_title_row = next_row
        next_row = _write_table(ws, fx_title_row, fx_df_to_write, title="FOREX RATES")

        # locate FX header row and number of FX rows
        fx_header_row = fx_title_row + 1
        fx_n_rows = len(fx_df_to_write) if fx_df_to_write is not None else 0
        fx_headers = list(fx_df_to_write.columns) if fx_n_rows > 0 else []

        # 5) Apply the same logic as equities but with 4 decimals (values untouched)
        _apply_price_decimal_format(ws, header_row=fx_header_row, n_rows=fx_n_rows, headers=fx_headers, decimals=4)

        # 6) timings block
        next_row += 2
        ws.Cells(next_row, 1).Value2 = "Timings"
        ws.Cells(next_row, 1).Font.Bold = True
        r = next_row + 1
        for k in ("Google", "Euronext", "ZoneBourse", "Forex", "Export", "Total"):
            if k in timings:
                ws.Cells(r, 1).Value2 = k
                ws.Cells(r, 2).Value2 = timings[k]
                r += 1

        if opened_here:
            wb.Save()
            wb.Close(SaveChanges=True)

        return sheet_name

    finally:
        pythoncom.CoUninitialize()

# ================== RUN ALL ==================
async def gather_all_data():
    if not GOOGLE_SYMBOLS and not EURONEXT_BATCH and not ZB_STOCKS and not FX_PAIRS:
        raise RuntimeError("No symbols loaded from CSV. Check the config file.")

    warnings: list[str] = []

    print("CONFIG  -  sources")
    print("Google Finance:", GOOGLE_SYMBOLS)
    print("Euronext:", [f"{e.get('ticker')} -> {e.get('url')}" for e in EURONEXT_BATCH])
    print("Zone Bourse:", [f'{t} -> {u}' for t,u in ZB_STOCKS])
    print("FX pairs:", FX_PAIRS)

    grand_t0 = time.perf_counter()
    httpx_mod, _http2 = _httpx_and_http2_flag()

    def _eq_empty_tuple():
        return pd.DataFrame(columns=["STOCK:EXCH","TICKER","PRICE","DATE"]), [], 0.0

    def _fx_empty_tuple():
        return pd.DataFrame(columns=["SOURCE","STOCK:EXCH","TICKER","PRICE","DATE","EXCHANGE","CURRENCY"]), [], 0.0

    def _process_result(label: str, payload, empty_factory):
        if payload is None:
            return empty_factory()
        if isinstance(payload, Exception):
            msg = f"[WARN] Section failed: {label}: {type(payload).__name__}: {payload}"
            print(msg)
            warnings.append(msg.replace("[WARN] ", ""))
            return empty_factory()
        return payload

    def _make_euronext_task():
        max_workers = min(10, max(1, len(EURONEXT_BATCH)))
        if httpx_mod is not None:
            return fetch_euronext_httpx(EURONEXT_BATCH, max_concurrency=max_workers)
        return asyncio.to_thread(fetch_euronext_requests, EURONEXT_BATCH, max_workers=max_workers)

    def _make_zb_task():
        return fetch_zonebourse_http(ZB_STOCKS, max_concurrency=min(8, len(ZB_STOCKS) or 1))

    def _make_fx_task():
        return asyncio.to_thread(fetch_fx_tradingview, FX_PAIRS)

    google_df_raw, g_times, g_elapsed = _eq_empty_tuple()
    euronext_df_raw, e_times, e_elapsed = _eq_empty_tuple()
    zb_df_raw, z_times, z_elapsed = _eq_empty_tuple()
    fx_df_raw, fx_times, fx_elapsed = _fx_empty_tuple()

    playwright_success = False
    if HAS_PLAYWRIGHT and async_playwright is not None:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                ctx = await browser.new_context(
                    user_agent=HEADERS["User-Agent"],
                    locale="en-US",
                    timezone_id="Europe/Paris",
                    viewport={"width": 1920, "height": 1080},
                    device_scale_factor=1,
                )

                tasks = []
                keys: list[str] = []

                def _add_task(key: str, coro):
                    tasks.append(coro)
                    keys.append(key)

                if GOOGLE_SYMBOLS:
                    _add_task("google", fetch_google(ctx, GOOGLE_SYMBOLS, concurrency=min(8, len(GOOGLE_SYMBOLS) or 1)))

                _add_task("euronext", _make_euronext_task())
                _add_task("zonebourse", _make_zb_task())
                _add_task("forex", _make_fx_task())

                results = await asyncio.gather(*tasks, return_exceptions=True)
                result_map = dict(zip(keys, results))

                google_df_raw, g_times, g_elapsed = _process_result("Google", result_map.get("google"), _eq_empty_tuple)
                euronext_df_raw, e_times, e_elapsed = _process_result("Euronext", result_map.get("euronext"), _eq_empty_tuple)
                zb_df_raw, z_times, z_elapsed = _process_result("Zone Bourse", result_map.get("zonebourse"), _eq_empty_tuple)
                fx_df_raw, fx_times, fx_elapsed = _process_result("Forex", result_map.get("forex"), _fx_empty_tuple)

                # ZoneBourse fallback with DOM if missing prices
                if not zb_df_raw.empty and zb_df_raw["PRICE"].isna().any():
                    missing = []
                    for _, row in zb_df_raw[zb_df_raw["PRICE"].isna()].iterrows():
                        tick = str(row["STOCK:EXCH"]).split(":",1)[0]
                        url = next((u for (t,u) in ZB_STOCKS if t.upper()==tick.upper()), None)
                        if url: missing.append((tick, url))
                    if missing:
                        fb = await fetch_zonebourse_playwright_dom(ctx, missing)
                        for i, r in zb_df_raw.iterrows():
                            if pd.isna(r["PRICE"]):
                                tick = str(r["STOCK:EXCH"]).split(":",1)[0].upper()
                                price, cur = fb.get(tick, (None, None))
                                if price is not None:
                                    exch = ZB_EXCHANGES.get(tick, " - ")
                                    ts = datetime.now(PARIS)
                                    zb_df_raw.at[i, "PRICE"] = float(price)
                                    zb_df_raw.at[i, "DATE"]  = f"{ts.strftime('%b %d, %I:%M:%S %p').lstrip('0')} {gmt_suffix_for(ts)} | {(cur or EXCHANGE_TO_CURRENCY.get(exch,''))} | {exch} | Source: Zone Bourse"

                await browser.close()
                playwright_success = True
        except Exception as exc:  # noqa: BLE001
            msg = f"Playwright run failed ({type(exc).__name__}: {exc}); falling back to HTTP-only mode."
            print(f"[WARN] {msg}")
            warnings.append(msg)

    if not playwright_success:
        if GOOGLE_SYMBOLS:
            msg = "Playwright not available; skipping Google Finance scrape."
            print(f"[WARN] {msg}")
            warnings.append(msg)

        results = await asyncio.gather(
            _make_euronext_task(),
            _make_zb_task(),
            _make_fx_task(),
            return_exceptions=True,
        )

        euronext_df_raw, e_times, e_elapsed = _process_result("Euronext", results[0], _eq_empty_tuple)
        zb_df_raw, z_times, z_elapsed = _process_result("Zone Bourse", results[1], _eq_empty_tuple)
        fx_df_raw, fx_times, fx_elapsed = _process_result("Forex", results[2], _fx_empty_tuple)

        if not zb_df_raw.empty and zb_df_raw["PRICE"].isna().any():
            warn = "ZoneBourse DOM fallback skipped because Playwright is unavailable; some ZoneBourse prices may remain empty."
            print(f"[WARN] {warn}")
            warnings.append(warn)

    # Standardize and combine equities only
    google_df   = std_cols(google_df_raw,  source="Google Finance")
    euronext_df = std_cols(euronext_df_raw,source="Euronext")
    zb_df       = std_cols(zb_df_raw,      source="Zone Bourse")

    equities_combined = pd.concat([google_df, euronext_df, zb_df], ignore_index=True)
    equities_combined = split_date_meta(equities_combined)
    equities_combined["PRICE"] = pd.to_numeric(equities_combined["PRICE"], errors="coerce")

    def _fix_currency(row):
        cur = str(row.get("CURRENCY","")).strip()
        exch = str(row.get("EXCHANGE","")).strip()
        if (not cur) or (cur == exch) or (len(cur) < 2) or (len(cur) > 4):
            return EXCHANGE_TO_CURRENCY.get(exch, cur or "")
        return cur
    if not equities_combined.empty:
        equities_combined["CURRENCY"] = equities_combined.apply(_fix_currency, axis=1)
    equities_combined, yahoo_warns = attach_yahoo_change(equities_combined)
    if not equities_combined.empty:
        prev_close = pd.to_numeric(equities_combined.get("Y_PREV_CLOSE"), errors="coerce")
        price = equities_combined["PRICE"]
        with np.errstate(divide="ignore", invalid="ignore"):
            pct = (price - prev_close) / prev_close * 100.0
        equities_combined["CHANGE"] = pct
        equities_combined.drop(columns=["Y_PREV_CLOSE"], inplace=True, errors="ignore")
    warnings.extend(yahoo_warns)

    desired_order = ["SOURCE","STOCK:EXCH","TICKER","PRICE","CHANGE","DATE","EXCHANGE","CURRENCY"]
    existing_order = [c for c in desired_order if c in equities_combined.columns]
    remaining_cols = [c for c in equities_combined.columns if c not in existing_order]
    equities_combined = equities_combined[existing_order + remaining_cols]

    # FX table stays separate; DATE already timestamp-only; PRICE numeric & unmodified
    forex_df = fx_df_raw.copy()
    forex_df, fx_change_warns = attach_forex_change(forex_df)
    warnings.extend(fx_change_warns)

    total_elapsed = time.perf_counter() - grand_t0

    pd.set_option("display.width", 220)
    pd.set_option("display.max_colwidth", 200)
    print("\n=== Equities head ===")
    print(equities_combined.head().to_string(index=False))
    print("\n=== Forex head ===")
    print(forex_df.head().to_string(index=False))

    eq_count = len(equities_combined)
    fx_count = len(forex_df)

    timings = {
        "Google":     f"{fmt_dur(g_elapsed)} (avg {fmt_dur(g_elapsed / max(1, len(GOOGLE_SYMBOLS)))} over {len(GOOGLE_SYMBOLS)})",
        "Euronext":   f"{fmt_dur(e_elapsed)} (avg {fmt_dur(e_elapsed / max(1, len(EURONEXT_BATCH)))} over {len(EURONEXT_BATCH)})",
        "ZoneBourse": f"{fmt_dur(z_elapsed)} (avg {fmt_dur(z_elapsed / max(1, len(ZB_STOCKS)))} over {len(ZB_STOCKS)})",
        "Forex":      f"{fmt_dur(fx_elapsed)} (avg {fmt_dur(fx_elapsed / max(1, len(FX_PAIRS)))} over {len(FX_PAIRS)})",
        "Export":     "0:00:00.000",
        "Total":      fmt_dur(total_elapsed),
    }

    return {
        "equities": equities_combined,
        "forex": forex_df,
        "timings": timings,
        "eq_count": eq_count,
        "fx_count": fx_count,
        "total_elapsed": total_elapsed,
        "warnings": warnings,
    }

async def run_all_and_push(target_workbook: str):
    data = await gather_all_data()
    equities_combined = data["equities"]
    forex_df = data["forex"]
    timings = data["timings"]
    eq_count = data["eq_count"]
    fx_count = data["fx_count"]
    warnings = data.get("warnings") or []

    sheet_name = write_two_tables_to_excel_new_sheet(target_workbook, equities_combined, forex_df, timings)
    print(f"\nWrote {eq_count} equity rows and {fx_count} forex rows to sheet: {sheet_name}")
    if warnings:
        print("\nWarnings during refresh:")
        for msg in warnings:
            print(f" - {msg}")
    return sheet_name

# ================== ENTRY POINT ==================
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("ERROR: Please pass the Excel workbook FullName or Name as the first argument.")
        print('Example: python lfpi_from_localcsv.py "C:\\Users\\jacques.rive\\LFPI_Portoflio_Stocks.xlsm"')
        sys.exit(2)

    try:
        load_config_from_local_csv(CONFIG_CSV_PATH)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(4)

    target = sys.argv[1]
    asyncio.run(run_all_and_push(target))
