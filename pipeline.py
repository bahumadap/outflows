"""
Arch Finance - Wind-Down Monitor
Data Pipeline: Extract from Dune (API or CSV), process, and consolidate.

Usage:
    # With Dune API key:
    python pipeline.py --api-key YOUR_DUNE_API_KEY

    # With pre-downloaded CSVs:
    python pipeline.py --csv-dir ./data/raw/

    # Both (API first, CSV fallback):
    python pipeline.py --api-key YOUR_KEY --csv-dir ./data/raw/
"""

import os
import sys
import json
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from config import (
    ALL_TOKENS_POLYGON, TOKENS_ETHEREUM,
    ARCHEMIST_TOKENS_POLYGON, ARCHEMIST_ADDR_TO_SYMBOL,
    ARCH_CONTRACTS_POLYGON, ARCH_CONTRACTS_ETHEREUM,
    SYMBOL_TO_BASE, BURN_ADDRESS,
    CUTOFF_DATE, DUST_THRESHOLD_USD, DUST_THRESHOLD_TOKENS,
    DUNE_API_BASE, DUNE_QUERY_SUPPLY_ETH, DUNE_QUERY_SUPPLY_POL, DUNE_QUERY_POOLS,
    DUNE_QUERY_BALANCES_POLYGON, DUNE_QUERY_OUTFLOWS_POLYGON,
    DUNE_QUERY_BALANCES_ETHEREUM, DUNE_QUERY_OUTFLOWS_ETHEREUM,
    VAULT_POSITIONS, GSHEET_PRICES_CSV, GSHEET_PRICE_COLUMNS,
    classify_outflow_destination,
)
from dune_queries import (
    query_balances_polygon, query_balances_ethereum,
    query_outflows_polygon, query_outflows_ethereum,
    query_usdc_inflows_polygon,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

for d in [RAW_DIR, PROCESSED_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Archivos que guardan el último timestamp procesado por red
LAST_TS_FILE = {
    "polygon":  RAW_DIR / "last_outflow_ts_polygon.txt",
    "ethereum": RAW_DIR / "last_outflow_ts_ethereum.txt",
}


def _load_last_ts(network: str) -> str:
    """Lee el último timestamp de outflows procesado. Si no existe, usa CUTOFF_DATE."""
    path = LAST_TS_FILE[network]
    if path.exists():
        ts = path.read_text().strip()
        log.info(f"  Último timestamp outflows ({network}): {ts}")
        return ts
    return f"{CUTOFF_DATE} 00:00:00"


def _save_last_ts(network: str, df: pd.DataFrame):
    """Guarda el max(block_time) de los nuevos outflows como próximo punto de partida."""
    if df.empty or "block_time" not in df.columns:
        return
    max_ts = pd.to_datetime(df["block_time"], errors="coerce").max()
    if pd.isna(max_ts):
        return
    ts_str = max_ts.strftime("%Y-%m-%d %H:%M:%S")
    LAST_TS_FILE[network].write_text(ts_str)
    log.info(f"  Guardado nuevo timestamp ({network}): {ts_str}")


# =============================================================================
# 1. WALLET LOADER
# =============================================================================
def _clean_address(addr: str) -> str:
    """Strip prefixes like eth:, matic:, strip whitespace, lowercase."""
    return addr.replace("eth:", "").replace("matic:", "").strip().lower()


def load_wallets(
    excel_path: str = None,
    preferentes_csv: str = None,
    retail_csv: str = None,
) -> pd.DataFrame:
    """
    Load and normalize the client wallet list.
    Accepts either an Excel file OR two CSVs (preferentes + retail).
    Auto-detects which files exist if paths are not provided.

    Returns a DataFrame with columns:
        wallet_address, customer_name, email, segment, network
    One row per wallet-network combination.
    """
    # --- Auto-detect files ---
    if excel_path is None and preferentes_csv is None:
        candidates_excel = [
            "wallets/clientes.xlsx", "clientes.xlsx",
            "../wallets/clientes.xlsx",
        ]
        candidates_pref = [
            "wallets/Preferentes.csv", "Preferentes.csv",
            "wallets/preferentes.csv", "preferentes.csv",
        ]
        candidates_retail = [
            "wallets/Retail.csv", "Retail.csv",
            "wallets/retail.csv", "retail.csv",
        ]
        for p in candidates_excel:
            if Path(p).exists():
                excel_path = p
                break
        if not excel_path:
            for p in candidates_pref:
                if Path(p).exists():
                    preferentes_csv = p
                    break
            for p in candidates_retail:
                if Path(p).exists():
                    retail_csv = p
                    break

    records = []

    # --- Load from Excel ---
    if excel_path and Path(excel_path).exists():
        log.info(f"Loading wallets from Excel: {excel_path}")
        xls = pd.ExcelFile(excel_path)
        if "Preferentes" in xls.sheet_names:
            df_pref = pd.read_excel(xls, "Preferentes")
            records += _parse_preferentes_df(df_pref)
        if "Retail" in xls.sheet_names:
            df_retail = pd.read_excel(xls, "Retail")
            records += _parse_retail_df(df_retail)

    # --- Load from CSVs ---
    elif preferentes_csv and Path(preferentes_csv).exists():
        log.info(f"Loading wallets from CSVs: {preferentes_csv}, {retail_csv}")
        df_pref = pd.read_csv(preferentes_csv)
        records += _parse_preferentes_df(df_pref)
        if retail_csv and Path(retail_csv).exists():
            df_retail = pd.read_csv(retail_csv)
            records += _parse_retail_df(df_retail)
    else:
        log.error("No wallet file found. Provide --wallets, --pref-csv, or --retail-csv")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["wallet_address"] = df["wallet_address"].str.lower().str.strip()
    df = df[df["wallet_address"].str.startswith("0x")]  # sanity check
    df = df.drop_duplicates(subset=["wallet_address", "network"])
    log.info(f"Loaded {len(df)} wallet-network entries "
             f"({df[df.segment=='preferente'].shape[0]} preferente, "
             f"{df[df.segment=='retail'].shape[0]} retail)")
    return df


def _parse_preferentes_df(df: pd.DataFrame) -> list:
    records = []
    for _, row in df.iterrows():
        name = str(row.get("Titular", "") or "").strip()
        email = str(row.get("Email", "") or "").strip()
        eth_addr = str(row.get("Address:ETH", "N/A") or "N/A").strip()
        poly_addr = str(row.get("Address:POLY", "N/A") or "N/A").strip()

        if eth_addr not in ("N/A", "", "None", "nan"):
            records.append({
                "wallet_address": _clean_address(eth_addr),
                "customer_name": name,
                "email": email,
                "segment": "preferente",
                "network": "ethereum",
            })
        if poly_addr not in ("N/A", "", "None", "nan"):
            records.append({
                "wallet_address": _clean_address(poly_addr),
                "customer_name": name,
                "email": email,
                "segment": "preferente",
                "network": "polygon",
            })
    return records


def _parse_retail_df(df: pd.DataFrame) -> list:
    records = []
    col_addr = "address" if "address" in df.columns else df.columns[0]
    col_email = "email" if "email" in df.columns else (df.columns[1] if len(df.columns) > 1 else None)
    for _, row in df.iterrows():
        addr = str(row.get(col_addr, "") or "").strip().lower()
        email = str(row.get(col_email, "") or "").strip() if col_email else ""
        if addr and addr != "nan" and addr.startswith("0x"):
            records.append({
                "wallet_address": addr,
                "customer_name": "",
                "email": email,
                "segment": "retail",
                "network": "polygon",
            })
    return records


# =============================================================================
# 2. DUNE API CLIENT
# =============================================================================
class DuneClient:
    """Minimal Dune API v1 client."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"X-Dune-API-Key": api_key}

    def execute_query(self, query_id: int, params: dict = None) -> str:
        """Execute a saved query and return execution_id."""
        url = f"{DUNE_API_BASE}/query/{query_id}/execute"
        body = {}
        if params:
            body["query_parameters"] = params
        r = requests.post(url, headers=self.headers, json=body)
        r.raise_for_status()
        return r.json()["execution_id"]

    def execute_sql(self, sql: str) -> str:
        """Execute raw SQL via the Dune API (requires Plus plan)."""
        url = f"{DUNE_API_BASE}/query/execute"
        # For custom SQL, we need to create a query first or use the execute endpoint
        # Using the /query/execute with raw SQL (available on Plus+)
        r = requests.post(url, headers=self.headers, json={"query_sql": sql})
        if r.status_code == 200:
            return r.json()["execution_id"]
        else:
            log.warning(f"Raw SQL execution not available (status {r.status_code}). "
                        "You may need to save queries in Dune UI and use query IDs.")
            raise RuntimeError(f"Dune API error: {r.text}")

    def wait_for_result(self, execution_id: str, timeout: int = 600, poll: int = 5) -> pd.DataFrame:
        """Poll until execution completes and return results as DataFrame."""
        url = f"{DUNE_API_BASE}/execution/{execution_id}/results"
        start = time.time()
        while time.time() - start < timeout:
            r = requests.get(url, headers=self.headers)
            r.raise_for_status()
            data = r.json()
            state = data.get("state", "")
            if state == "QUERY_STATE_COMPLETED":
                rows = data.get("result", {}).get("rows", [])
                return pd.DataFrame(rows)
            elif state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
                raise RuntimeError(f"Query failed: {data}")
            log.info(f"  Query state: {state}, waiting {poll}s...")
            time.sleep(poll)
        raise TimeoutError(f"Query did not complete within {timeout}s")

    def get_latest_results(self, query_id: int, limit: int = 50000) -> pd.DataFrame:
        """
        Obtiene los últimos resultados cacheados de una query SIN ejecutarla.
        No consume créditos de ejecución — solo lee lo que ya corrió.
        Requiere que la query tenga al menos un run previo (manual o programado en Dune).
        """
        url = f"{DUNE_API_BASE}/query/{query_id}/results"
        params = {"limit": limit}
        r = requests.get(url, headers=self.headers, params=params)
        if r.status_code == 200:
            data = r.json()
            rows = data.get("result", {}).get("rows", [])
            df = pd.DataFrame(rows)
            # Log cuándo fue ejecutada por última vez
            meta = data.get("execution_started_at") or data.get("result", {}).get("metadata", {})
            log.info(f"  Resultados cacheados query {query_id}: {len(df)} rows")
            return df
        elif r.status_code == 404:
            raise RuntimeError(f"Query {query_id} no tiene resultados cacheados. Córrela al menos una vez en Dune.")
        else:
            raise RuntimeError(f"Dune API error {r.status_code}: {r.text}")

    def run_query_id(self, query_id: int, params: dict = None) -> pd.DataFrame:
        """Ejecuta una query por ID (consume créditos). Usar solo si no hay caché."""
        log.info(f"Ejecutando Dune query {query_id}...")
        exec_id = self.execute_query(query_id, params)
        return self.wait_for_result(exec_id)

    def run_sql(self, sql: str) -> pd.DataFrame:
        """Execute raw SQL and return results."""
        log.info("Executing custom SQL on Dune...")
        exec_id = self.execute_sql(sql)
        return self.wait_for_result(exec_id)


# =============================================================================
# 3. DATA EXTRACTION
# =============================================================================
def _cache_is_fresh(name: str, max_hours: float) -> bool:
    """True si el CSV existe y fue modificado hace menos de max_hours."""
    path = RAW_DIR / f"{name}.csv"
    if not path.exists():
        return False
    age_hours = (datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)).total_seconds() / 3600
    return age_hours < max_hours


# Queries que se ejecutan diario (precios y balances cambian frecuentemente)
# NOTA: outflows_polygon y outflows_ethereum se manejan en el bloque incremental — NO incluir aquí
QUERIES_DAILY = [
    ("pools",              DUNE_QUERY_POOLS),
    ("balances_polygon",   DUNE_QUERY_BALANCES_POLYGON),
]

# Queries que se ejecutan semanal (cambian poco)
# Nota: precios ya no vienen de Dune — se obtienen del Google Sheet (gratis)
QUERIES_WEEKLY = [
    ("supply_eth",         DUNE_QUERY_SUPPLY_ETH),   # Supply Ethereum (tokens base)
    ("supply_pol",         DUNE_QUERY_SUPPLY_POL),   # Supply Polygon (todos los tokens)
    ("balances_ethereum",  DUNE_QUERY_BALANCES_ETHEREUM),
]


def extract_via_api(dune: DuneClient) -> dict:
    """
    Extract data from Dune API con caché inteligente:
    - Queries diarias: se saltean si el CSV tiene < 20 horas
    - Queries semanales: se saltean si el CSV tiene < 7 días
    Esto ahorra créditos de Dune en runs consecutivos del mismo día.
    """
    results = {}

    # ── Queries con caché (no incrementales) ─────────────────────────────
    cached_queries = [
        *[(name, qid, 20)   for name, qid in QUERIES_DAILY],   # max 20h
        *[(name, qid, 168)  for name, qid in QUERIES_WEEKLY],  # max 7 días
    ]

    for name, query_id, max_hours in cached_queries:
        path = RAW_DIR / f"{name}.csv"
        if _cache_is_fresh(name, max_hours):
            cached = pd.read_csv(path)
            results[name] = cached
            log.info(f"  ✓ {name}: caché fresca ({len(cached)} rows, < {max_hours}h)")
            continue
        try:
            log.info(f"--- Leyendo resultados cacheados query {query_id} ({name}) ---")
            df = dune.get_latest_results(query_id)
            results[name] = df
            if not df.empty:
                df.to_csv(path, index=False)
        except Exception as e:
            log.error(f"  ✗ Failed {name}: {e}")
            results[name] = pd.read_csv(path) if path.exists() else pd.DataFrame()
            if path.exists():
                log.warning(f"  ↩ Usando caché local para {name}")

    # ── Outflows incrementales ────────────────────────────────────────────
    # Solo pide transfers nuevos desde el último timestamp procesado.
    # Acumula resultados en el CSV existente (no reemplaza).
    incremental = [
        ("outflows_polygon",  DUNE_QUERY_OUTFLOWS_POLYGON,  "polygon"),
        ("outflows_ethereum", DUNE_QUERY_OUTFLOWS_ETHEREUM, "ethereum"),
    ]

    for name, query_id, network in incremental:
        path = RAW_DIR / f"{name}.csv"
        last_ts = _load_last_ts(network)
        try:
            log.info(f"--- Outflows cacheados {network} (último run en Dune) ---")
            df_new = dune.get_latest_results(query_id)
            log.info(f"  ✓ {name}: {len(df_new)} rows nuevos desde Dune")

            # Acumular con los datos históricos existentes
            if path.exists():
                df_existing = pd.read_csv(path)
                if not df_new.empty:
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    # Deduplicar por (tx_hash, wallet_from, symbol) — NO solo tx_hash
                    # porque una tx batch puede tener transfers de múltiples wallets/tokens
                    dedup_cols = [c for c in ["tx_hash", "wallet_from", "symbol"] if c in df_combined.columns]
                    if dedup_cols:
                        df_combined = df_combined.drop_duplicates(subset=dedup_cols)
                    results[name] = df_combined
                    df_combined.to_csv(path, index=False)
                    log.info(f"  Acumulado: {len(df_combined)} rows totales en {name}")
                else:
                    results[name] = df_existing
                    log.info(f"  Sin datos nuevos, usando existentes ({len(df_existing)} rows)")
            else:
                results[name] = df_new
                if not df_new.empty:
                    df_new.to_csv(path, index=False)

            # Guardar nuevo timestamp solo si hubo datos nuevos
            if not df_new.empty:
                _save_last_ts(network, df_new)

        except Exception as e:
            log.error(f"  ✗ Failed {name}: {e}")
            results[name] = pd.read_csv(path) if path.exists() else pd.DataFrame()
            if path.exists():
                log.warning(f"  ↩ Usando caché vieja para {name}")

    return results


def extract_via_csv(csv_dir: str) -> dict:
    """Load pre-downloaded CSVs from the specified directory."""
    results = {}
    csv_path = Path(csv_dir)

    expected_files = [
        "prices", "supply", "pools",
        "balances_polygon", "balances_ethereum",
        "outflows_polygon", "outflows_ethereum",
    ]

    for name in expected_files:
        path = csv_path / f"{name}.csv"
        if path.exists():
            results[name] = pd.read_csv(path)
            log.info(f"  Loaded {name} from {path} ({len(results[name])} rows)")
        else:
            log.warning(f"  Missing: {path}")

    return results


# =============================================================================
# 4. DATA PROCESSING
# =============================================================================
def fetch_gsheet_prices() -> dict:
    """
    Descarga el CSV de precios desde Google Sheets y retorna el último precio
    disponible por token (última fila con fecha).
    Gratis, sin créditos de Dune. Fuente primaria de precios.
    """
    try:
        resp = requests.get(GSHEET_PRICES_CSV, timeout=15)
        resp.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
        # Tomar la última fila no vacía
        df = df.dropna(subset=["Date"])
        if df.empty:
            log.warning("Google Sheet de precios vacío")
            return {}
        last = df.iloc[-1]
        prices = {}
        for col in GSHEET_PRICE_COLUMNS:
            if col in df.columns:
                try:
                    val = float(last[col])
                    if val > 0:
                        prices[col] = val
                except (ValueError, TypeError):
                    pass
        date_str = str(last.get("Date", "?"))
        log.info(f"Precios desde Google Sheet ({date_str}): { {k: round(v,4) for k,v in prices.items()} }")
        return prices
    except Exception as e:
        log.error(f"No se pudo obtener precios de Google Sheet: {e}")
        return {}


def process_prices(df_prices: pd.DataFrame = None, df_pools: pd.DataFrame = None) -> dict:
    """
    Process prices into a symbol -> USD price dict.

    Sources (in order of priority):
    1. Google Sheet (fuente primaria, gratis) — incluye vault tokens AAGG/AMOD/ABAL directamente.
    2. Pools data (query 3591853) — fallback para tokens base si el sheet falla.
    3. VAULT_POSITIONS NAV — fallback si vault tokens no están en ninguna fuente.
    4. SYMBOL_TO_BASE propagation — extiende precios a variantes _PROD, _V1, _SET.
    """
    prices = {}

    # --- Source 1: Google Sheet (primaria, sin créditos Dune) ---
    gsheet_prices = fetch_gsheet_prices()
    prices.update(gsheet_prices)

    # --- Source 2: pools data (fallback para tokens base) ---
    if df_pools is not None and not df_pools.empty:
        for _, row in df_pools.iterrows():
            token = str(row.get("token", "")).strip().upper()
            if not token or token == "USDC":
                continue
            if token in prices:
                continue  # ya tenemos del sheet — no pisar
            try:
                price = float(row.get("price", 0) or 0)
            except (ValueError, TypeError):
                price = 0
            if price > 0:
                prices[token] = price
                log.info(f"  Precio desde pools (fallback): {token} = ${price:.4f}")

    if not prices:
        log.warning("No price data found. All USD values will be $0.")
        return {}

    # --- Source 3: NAV de vault tokens (fallback si no están en sheet ni pools) ---
    for vault_sym, components in VAULT_POSITIONS.items():
        if vault_sym in prices:
            continue  # ya tenemos del sheet
        nav = sum(units * prices.get(comp, 0) for comp, units in components.items())
        if nav > 0:
            prices[vault_sym] = nav
            log.info(f"  Vault NAV (fallback): {vault_sym} = ${nav:.4f}")

    # --- Source 4: propagar a variantes (_PROD, _V1, _SET) ---
    extended = {}
    for sym, base in SYMBOL_TO_BASE.items():
        if sym not in prices and base in prices:
            extended[sym] = prices[base]
    prices.update(extended)

    log.info(f"Final prices ({len(prices)} symbols): { {k: round(v,4) for k,v in sorted(prices.items())} }")
    return prices


def process_balances(
    df_bal_poly: pd.DataFrame,
    df_bal_eth: pd.DataFrame,
    df_wallets: pd.DataFrame,
    prices: dict,
) -> pd.DataFrame:
    """
    Process balance data: filter to monitored wallets, add client info, compute USD values.
    Returns: wallet_address, token_address, symbol, base_symbol, balance, price_usd, value_usd,
             network, segment, customer_name, email
    """
    frames = []

    # Process Polygon balances
    if df_bal_poly is not None and not df_bal_poly.empty:
        df = df_bal_poly.copy()
        df["wallet"] = df["wallet"].str.lower()
        df["network"] = "polygon"
        frames.append(df)

    # Process Ethereum balances
    if df_bal_eth is not None and not df_bal_eth.empty:
        df = df_bal_eth.copy()
        df["wallet"] = df["wallet"].str.lower()
        df["network"] = "ethereum"
        frames.append(df)

    if not frames:
        log.warning("No balance data available")
        return pd.DataFrame()

    df_all = pd.concat(frames, ignore_index=True)

    # Filter to monitored wallets only
    monitored = set(df_wallets["wallet_address"].str.lower())
    df_filtered = df_all[df_all["wallet"].isin(monitored)].copy()
    log.info(f"Balances: {len(df_all)} total holders -> {len(df_filtered)} monitored wallets")

    # Add base symbol for pricing
    df_filtered["base_symbol"] = df_filtered["symbol"].map(SYMBOL_TO_BASE).fillna(df_filtered["symbol"])

    # Add USD value
    df_filtered["price_usd"] = df_filtered["base_symbol"].map(prices).fillna(0)
    df_filtered["value_usd"] = df_filtered["balance"] * df_filtered["price_usd"]

    # Merge with client info
    df_merged = df_filtered.merge(
        df_wallets[["wallet_address", "customer_name", "email", "segment", "network"]],
        left_on=["wallet", "network"],
        right_on=["wallet_address", "network"],
        how="left",
    )

    # For wallets that appear in the balance data but didn't match on network,
    # try matching on wallet_address alone
    unmatched = df_merged[df_merged["segment"].isna()]
    if len(unmatched) > 0:
        log.info(f"  {len(unmatched)} balance rows unmatched after network join, trying wallet-only join")
        wallet_info = df_wallets.drop_duplicates("wallet_address")[
            ["wallet_address", "customer_name", "email", "segment"]
        ]
        for idx in unmatched.index:
            w = df_merged.loc[idx, "wallet"]
            match = wallet_info[wallet_info["wallet_address"] == w]
            if not match.empty:
                df_merged.loc[idx, "customer_name"] = match.iloc[0]["customer_name"]
                df_merged.loc[idx, "email"] = match.iloc[0]["email"]
                df_merged.loc[idx, "segment"] = match.iloc[0]["segment"]

    # Clean up
    df_merged = df_merged.drop(columns=["wallet_address"], errors="ignore")
    df_merged = df_merged.rename(columns={"wallet": "wallet_address"})

    # Filter dust
    df_merged = df_merged[df_merged["balance"] > DUST_THRESHOLD_TOKENS]

    return df_merged


def process_outflows(
    df_out_poly: pd.DataFrame,
    df_out_eth: pd.DataFrame,
    df_wallets: pd.DataFrame,
    prices: dict,
) -> pd.DataFrame:
    """
    Process outflow data: filter to monitored wallets, classify destinations, compute USD.
    """
    frames = []

    if df_out_poly is not None and not df_out_poly.empty:
        df = df_out_poly.copy()
        df["wallet_from"] = df["wallet_from"].str.lower()
        df["wallet_to"] = df["wallet_to"].str.lower()
        df["network"] = "polygon"
        frames.append(df)

    if df_out_eth is not None and not df_out_eth.empty:
        df = df_out_eth.copy()
        df["wallet_from"] = df["wallet_from"].str.lower()
        df["wallet_to"] = df["wallet_to"].str.lower()
        df["network"] = "ethereum"
        frames.append(df)

    if not frames:
        log.warning("No outflow data available")
        return pd.DataFrame()

    df_all = pd.concat(frames, ignore_index=True)

    # Filter: only outflows FROM monitored wallets
    monitored = set(df_wallets["wallet_address"].str.lower())
    # Also build set of monitored wallets to exclude internal transfers
    df_filtered = df_all[df_all["wallet_from"].isin(monitored)].copy()
    log.info(f"Outflows: {len(df_all)} total transfers -> {len(df_filtered)} from monitored wallets")

    # Classify destination
    df_filtered["destination_type"] = df_filtered["wallet_to"].apply(classify_outflow_destination)

    # Flag internal transfers (from one client to another client)
    df_filtered["is_internal_transfer"] = df_filtered["wallet_to"].isin(monitored)

    # Add base symbol for pricing
    df_filtered["base_symbol"] = df_filtered["symbol"].map(SYMBOL_TO_BASE).fillna(df_filtered["symbol"])

    # Add USD value
    df_filtered["price_usd"] = df_filtered["base_symbol"].map(prices).fillna(0)
    df_filtered["value_usd"] = df_filtered["amount"] * df_filtered["price_usd"]

    # Merge client info
    df_filtered = df_filtered.merge(
        df_wallets[["wallet_address", "customer_name", "email", "segment"]].drop_duplicates("wallet_address"),
        left_on="wallet_from",
        right_on="wallet_address",
        how="left",
    ).drop(columns=["wallet_address"], errors="ignore")

    # Parse timestamps
    if "block_time" in df_filtered.columns:
        df_filtered["block_time"] = pd.to_datetime(df_filtered["block_time"], errors="coerce")

    return df_filtered


# =============================================================================
# 5. METRICS COMPUTATION
# =============================================================================
def compute_wallet_summary(
    df_balances: pd.DataFrame,
    df_outflows: pd.DataFrame,
    df_wallets: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Compute per-wallet summary combining balances and outflows.
    Returns one row per wallet with all key metrics.

    df_wallets is used to fill in segment/customer_name/email for wallets
    that fully withdrew (zero balance) and thus only appear in outflows.
    """
    if df_balances.empty and df_outflows.empty:
        return pd.DataFrame()

    # Balance aggregation per wallet
    if not df_balances.empty:
        bal_agg = df_balances.groupby("wallet_address").agg(
            total_balance_usd=("value_usd", "sum"),
            num_tokens=("symbol", "nunique"),
            tokens_held=("symbol", lambda x: ", ".join(sorted(x.unique()))),
            network=("network", "first"),
            segment=("segment", "first"),
            customer_name=("customer_name", "first"),
            email=("email", "first"),
        ).reset_index()
    else:
        bal_agg = pd.DataFrame(columns=[
            "wallet_address", "total_balance_usd", "num_tokens",
            "tokens_held", "network", "segment", "customer_name", "email"
        ])

    # Outflow aggregation per wallet (excluding internal transfers)
    if not df_outflows.empty:
        real_outflows = df_outflows[~df_outflows["is_internal_transfer"]]
        out_agg = real_outflows.groupby("wallet_from").agg(
            total_outflow_usd=("value_usd", "sum"),
            num_outflow_events=("tx_hash", "nunique"),
            last_outflow_date=("block_time", "max"),
            outflow_tokens=("symbol", lambda x: ", ".join(sorted(x.unique()))),
        ).reset_index().rename(columns={"wallet_from": "wallet_address"})
    else:
        out_agg = pd.DataFrame(columns=[
            "wallet_address", "total_outflow_usd", "num_outflow_events",
            "last_outflow_date", "outflow_tokens"
        ])

    # Base: TODOS los wallets registrados (incluso los que retiraron antes del cutoff)
    if df_wallets is not None and not df_wallets.empty:
        base = df_wallets[["wallet_address", "customer_name", "email", "segment", "network"]].drop_duplicates("wallet_address")
        summary = base.merge(bal_agg, on="wallet_address", how="left")
        # Para columnas duplicadas de customer_name etc., usar las del CSV original
        for col in ["customer_name", "email", "segment", "network"]:
            if f"{col}_x" in summary.columns:
                summary[col] = summary[f"{col}_x"].fillna(summary.get(f"{col}_y", ""))
                summary = summary.drop(columns=[f"{col}_x", f"{col}_y"], errors="ignore")
        summary = summary.merge(out_agg, on="wallet_address", how="left")
    else:
        summary = bal_agg.merge(out_agg, on="wallet_address", how="outer")

    # Fill NaN numerics
    summary["total_balance_usd"] = summary["total_balance_usd"].fillna(0)
    summary["total_outflow_usd"] = summary["total_outflow_usd"].fillna(0)
    summary["num_outflow_events"] = summary["num_outflow_events"].fillna(0).astype(int)

    # --- Backfill segment/customer_name/email for fully-withdrawn wallets ---
    # These wallets have zero balance (absent from bal_agg) but appear in out_agg.
    # Their client info is still in df_wallets, so we pull it in here.
    if df_wallets is not None and not df_wallets.empty:
        missing_mask = summary["segment"].isna()
        if missing_mask.any():
            wallet_info = (
                df_wallets[["wallet_address", "customer_name", "email", "segment", "network"]]
                .drop_duplicates("wallet_address")
                .set_index("wallet_address")
            )
            for idx in summary[missing_mask].index:
                wa = summary.at[idx, "wallet_address"]
                if wa in wallet_info.index:
                    for col in ["customer_name", "email", "segment", "network"]:
                        summary.at[idx, col] = wallet_info.at[wa, col]
            filled = missing_mask.sum() - summary["segment"].isna().sum()
            log.info(f"Backfilled client info for {filled} fully-withdrawn wallets")

    # Classify wallet status
    def classify_status(row):
        has_balance = row["total_balance_usd"] > DUST_THRESHOLD_USD
        has_outflow = row["total_outflow_usd"] > DUST_THRESHOLD_USD
        if has_balance and not has_outflow:
            return "Sin movimiento"
        elif has_balance and has_outflow:
            return "Retiro parcial"
        elif not has_balance and has_outflow:
            return "Retirado completamente"
        else:
            return "Sin saldo (sin retiro detectado)"

    summary["status"] = summary.apply(classify_status, axis=1)

    # Compute withdrawal percentage
    total_per_wallet = summary["total_balance_usd"] + summary["total_outflow_usd"]
    summary["pct_withdrawn"] = (
        summary["total_outflow_usd"] / total_per_wallet.replace(0, 1) * 100
    ).round(2)

    return summary.sort_values("total_balance_usd", ascending=False)


def compute_global_metrics(wallet_summary: pd.DataFrame) -> dict:
    """Compute top-level KPIs for the dashboard."""
    if wallet_summary.empty:
        return {}

    total_remaining = wallet_summary["total_balance_usd"].sum()
    total_withdrawn = wallet_summary["total_outflow_usd"].sum()
    total_aum = total_remaining + total_withdrawn

    # By segment
    pref = wallet_summary[wallet_summary["segment"] == "preferente"]
    retail = wallet_summary[wallet_summary["segment"] == "retail"]

    metrics = {
        "total_remaining_usd": total_remaining,
        "total_withdrawn_usd": total_withdrawn,
        "total_initial_usd": total_aum,
        "pct_remaining": (total_remaining / max(total_aum, 1)) * 100,
        "pct_withdrawn": (total_withdrawn / max(total_aum, 1)) * 100,

        "preferente_remaining_usd": pref["total_balance_usd"].sum(),
        "preferente_withdrawn_usd": pref["total_outflow_usd"].sum(),
        "retail_remaining_usd": retail["total_balance_usd"].sum(),
        "retail_withdrawn_usd": retail["total_outflow_usd"].sum(),

        "wallets_with_balance": (wallet_summary["total_balance_usd"] > DUST_THRESHOLD_USD).sum(),
        "wallets_without_balance": (wallet_summary["total_balance_usd"] <= DUST_THRESHOLD_USD).sum(),
        "wallets_fully_withdrawn": (wallet_summary["status"] == "Retirado completamente").sum(),
        "wallets_partial_withdrawal": (wallet_summary["status"] == "Retiro parcial").sum(),
        "wallets_no_movement": (wallet_summary["status"] == "Sin movimiento").sum(),

        "total_wallets": len(wallet_summary),
        "timestamp": datetime.utcnow().isoformat(),
    }
    return metrics


# =============================================================================
# 5b. RECONCILIACIÓN: supply × precio vs suma total de todos los holders
# =============================================================================
def compute_reconciliation(
    df_supply: pd.DataFrame,             # supply total por token (de Dune)
    df_balances_all_raw: pd.DataFrame,   # no usado (mantenido por compatibilidad)
    df_balances_clients: pd.DataFrame,   # balances actuales de clientes registrados
    prices: dict,
    df_outflows: pd.DataFrame = None,    # retiros de clientes registrados
) -> pd.DataFrame:
    """
    Desglose por token usando los MISMOS datos que el overview (balances + outflows de clientes).
    Garantiza consistencia total: remanente aquí = remanente en la pantalla principal.

    Columnas:
      token         → símbolo base (WEB3, CHAIN, ABDY, etc.)
      precio_usd    → precio actual
      remanente_usd → lo que aún tienen los clientes (de balances.csv)
      retirado_usd  → lo que ya retiraron desde el 1 Abr (de outflows.csv, sin internos)
      historico_usd → remanente + retirado (AUM inicial estimado)
      pct_retirado  → % ya retirado del histórico
      alerta        → ✓ OK / ⚠️ si hay dato inconsistente
    """
    if df_balances_clients.empty or not prices:
        return pd.DataFrame()

    # ── 1. Remanente por token (desde balances de clientes) ───────────────────
    bal = df_balances_clients.copy()
    if "base_symbol" not in bal.columns:
        bal["base_symbol"] = bal["symbol"].map(SYMBOL_TO_BASE).fillna(bal["symbol"])

    remanente = (
        bal.groupby("base_symbol")
        .agg(
            remanente_tokens=("balance", "sum"),
            remanente_usd=("value_usd", "sum"),
            holders=("wallet_address", "nunique"),
        )
        .reset_index()
        .rename(columns={"base_symbol": "token"})
    )

    # ── 2. Retirado por token (desde outflows, sin transferencias internas) ───
    if df_outflows is not None and not df_outflows.empty:
        of = df_outflows[~df_outflows.get("is_internal_transfer", pd.Series(False, index=df_outflows.index))].copy()
        if "base_symbol" not in of.columns:
            of["base_symbol"] = of["symbol"].map(SYMBOL_TO_BASE).fillna(of["symbol"])
        retirado = (
            of.groupby("base_symbol")
            .agg(
                retirado_tokens=("amount", "sum"),
                retirado_usd=("value_usd", "sum"),
                num_txs=("tx_hash", "nunique"),
            )
            .reset_index()
            .rename(columns={"base_symbol": "token"})
        )
    else:
        retirado = pd.DataFrame(columns=["token", "retirado_tokens", "retirado_usd", "num_txs"])

    # ── 3. Supply más reciente por token y red ────────────────────────────────
    if not df_supply.empty and "label" in df_supply.columns and "supply" in df_supply.columns:
        sup = df_supply.copy()
        sup["day"] = pd.to_datetime(sup["day"], errors="coerce", utc=True)
        has_network = "network" in sup.columns

        if has_network:
            # Normalizar label usando SYMBOL_TO_BASE (ej. ABDY_V1 → ABDY)
            sup["label"] = sup["label"].map(SYMBOL_TO_BASE).fillna(sup["label"])
            # Supply por red — sumar variantes del mismo token y tomar el último valor
            latest_by_net = (
                sup.sort_values("day")
                .groupby(["label", "network", "day"])["supply"]
                .sum()
                .reset_index()
                .groupby(["label", "network"])
                .last()
                .reset_index()
            )
            # Pivot: columnas supply_ethereum y supply_polygon
            latest_supply = latest_by_net.pivot(
                index="label", columns="network", values="supply"
            ).reset_index()
            latest_supply.columns.name = None
            latest_supply = latest_supply.rename(columns={"label": "token"})
            if "ethereum" not in latest_supply.columns:
                latest_supply["ethereum"] = 0
            if "polygon" not in latest_supply.columns:
                latest_supply["polygon"] = 0
            latest_supply = latest_supply.rename(columns={
                "ethereum": "supply_eth",
                "polygon":  "supply_pol",
            })
            latest_supply["supply_total"] = latest_supply["supply_eth"].fillna(0) + latest_supply["supply_pol"].fillna(0)
        else:
            # Formato viejo sin columna network → todo es supply_total
            latest_supply = (
                sup.sort_values("day")
                .groupby("label")["supply"]
                .last()
                .reset_index()
                .rename(columns={"label": "token", "supply": "supply_total"})
            )
            latest_supply["supply_eth"] = 0
            latest_supply["supply_pol"] = 0
    else:
        latest_supply = pd.DataFrame(columns=["token", "supply_total", "supply_eth", "supply_pol"])

    # ── 4. Merge y cálculos ───────────────────────────────────────────────────
    rec = remanente.merge(retirado, on="token", how="outer").fillna(0)
    rec = rec.merge(latest_supply, on="token", how="left")
    for col in ["supply_total", "supply_eth", "supply_pol"]:
        if col not in rec.columns:
            rec[col] = 0
        rec[col] = rec[col].fillna(0)
    rec["precio_usd"]     = rec["token"].map(prices).fillna(0)
    rec["market_cap_eth"] = rec["supply_eth"] * rec["precio_usd"]
    rec["market_cap_pol"] = rec["supply_pol"] * rec["precio_usd"]
    rec["market_cap_usd"] = rec["supply_total"] * rec["precio_usd"]
    rec["historico_usd"] = rec["remanente_usd"] + rec["retirado_usd"]
    rec["pct_retirado"]  = rec.apply(
        lambda r: r["retirado_usd"] / r["historico_usd"] * 100
        if r["historico_usd"] > 0 else 0, axis=1
    ).round(2)
    rec["pct_remanente_vs_cap"] = rec.apply(
        lambda r: r["remanente_usd"] / r["market_cap_usd"] * 100
        if r["market_cap_usd"] > 0 else 0, axis=1
    ).round(1)

    rec["alerta"] = rec.apply(
        lambda r: "⚠️ Sin precio" if r["precio_usd"] == 0
        else ("⚠️ Remanente > Market Cap" if r["market_cap_usd"] > 0 and r["remanente_usd"] > r["market_cap_usd"] * 1.05
        else ("⚠️ Retirado > Histórico" if r["retirado_usd"] > r["historico_usd"] * 1.01
        else "✓ OK")),
        axis=1
    )

    rec = rec[(rec["precio_usd"] > 0) & (rec["historico_usd"] > 0)]
    return rec.sort_values("historico_usd", ascending=False).reset_index(drop=True)


# =============================================================================
# 5c. UNKNOWN WALLET DETECTION
# =============================================================================
def compute_unknown_wallets(
    df_bal_poly: pd.DataFrame,
    df_out_poly: pd.DataFrame,
    df_wallets: pd.DataFrame,
    prices: dict,
    extra_exclude: set = None,
) -> pd.DataFrame:
    """
    Find wallets that hold/held Arch tokens but are NOT in the client CSV.
    Excludes known Arch contracts and high-frequency destination addresses (contracts).
    Returns one row per unknown wallet with balance_usd, outflow_usd, tokens held.
    """
    if df_bal_poly.empty and df_out_poly.empty:
        return pd.DataFrame()

    registered = set(df_wallets["wallet_address"].str.lower())
    known_contracts = set(a.lower() for a in ARCH_CONTRACTS_POLYGON.keys())
    known_contracts.add(BURN_ADDRESS.lower())

    # Heuristic: addresses that received tokens from >10 different senders are likely contracts
    if not df_out_poly.empty:
        dest_counts = df_out_poly.groupby("wallet_to")["wallet_from"].nunique()
        likely_contracts = set(dest_counts[dest_counts > 10].index.str.lower())
    else:
        likely_contracts = set()

    exclude = registered | known_contracts | likely_contracts
    if extra_exclude:
        exclude |= {a.lower() for a in extra_exclude}

    # All wallets seen on-chain
    on_chain = set()
    if not df_bal_poly.empty:
        on_chain |= set(df_bal_poly["wallet"].str.lower())
    if not df_out_poly.empty:
        on_chain |= set(df_out_poly["wallet_from"].str.lower())

    unknown = on_chain - exclude

    if not unknown:
        return pd.DataFrame()

    # --- Balance side ---
    records = []
    if not df_bal_poly.empty:
        bal_unk = df_bal_poly[df_bal_poly["wallet"].str.lower().isin(unknown)].copy()
        bal_unk["base_sym"] = bal_unk["symbol"].map(SYMBOL_TO_BASE).fillna(bal_unk["symbol"])
        bal_unk["price_usd"] = bal_unk["base_sym"].map(prices).fillna(0)
        bal_unk["value_usd"] = bal_unk["balance"] * bal_unk["price_usd"]
        bal_agg = bal_unk.groupby("wallet").agg(
            balance_usd=("value_usd", "sum"),
            tokens=("symbol", lambda x: ", ".join(sorted(x.unique()))),
        ).reset_index().rename(columns={"wallet": "wallet_address"})
    else:
        bal_agg = pd.DataFrame(columns=["wallet_address", "balance_usd", "tokens"])

    # --- Outflow side ---
    if not df_out_poly.empty:
        out_unk = df_out_poly[df_out_poly["wallet_from"].str.lower().isin(unknown)].copy()
        out_unk["base_sym"] = out_unk["symbol"].map(SYMBOL_TO_BASE).fillna(out_unk["symbol"])
        out_unk["price_usd"] = out_unk["base_sym"].map(prices).fillna(0)
        out_unk["value_usd"] = out_unk["amount"] * out_unk["price_usd"]
        out_agg = out_unk.groupby("wallet_from").agg(
            outflow_usd=("value_usd", "sum"),
            last_activity=("block_time", "max"),
            num_txs=("tx_hash", "nunique"),
        ).reset_index().rename(columns={"wallet_from": "wallet_address"})
    else:
        out_agg = pd.DataFrame(columns=["wallet_address", "outflow_usd", "last_activity", "num_txs"])

    df = bal_agg.merge(out_agg, on="wallet_address", how="outer").fillna(0)
    df["total_aum"] = df["balance_usd"] + df["outflow_usd"]
    df = df[df["total_aum"] > 10].sort_values("balance_usd", ascending=False).reset_index(drop=True)

    log.info(f"Unknown wallets detected: {len(df)} "
             f"(${df['balance_usd'].sum():,.0f} rem, ${df['outflow_usd'].sum():,.0f} withdrawn)")
    return df


# =============================================================================
# 6. MAIN PIPELINE
# =============================================================================
def run_pipeline(
    api_key: Optional[str] = None,
    csv_dir: Optional[str] = None,
    wallets_path: str = None,
    preferentes_csv: str = None,
    retail_csv: str = None,
) -> dict:
    """
    Run the full pipeline:
    1. Load wallets
    2. Extract data (API or CSV)
    3. Process and consolidate
    4. Compute metrics
    5. Save processed datasets
    """
    log.info("=" * 60)
    log.info("  ARCH FINANCE WIND-DOWN MONITOR - PIPELINE START")
    log.info(f"  Cutoff date: {CUTOFF_DATE}")
    log.info(f"  Timestamp: {datetime.utcnow().isoformat()}")
    log.info("=" * 60)

    # 1. Load wallets
    df_wallets = load_wallets(
        excel_path=wallets_path,
        preferentes_csv=preferentes_csv,
        retail_csv=retail_csv,
    )
    df_wallets.to_csv(PROCESSED_DIR / "wallets_normalized.csv", index=False)

    # 2. Extract data
    raw_data = {}
    if api_key:
        try:
            dune = DuneClient(api_key)
            raw_data = extract_via_api(dune)
        except Exception as e:
            log.error(f"API extraction failed: {e}")

    if csv_dir and not raw_data:
        raw_data = extract_via_csv(csv_dir)
    elif csv_dir:
        # Supplement with CSV for any missing datasets
        csv_data = extract_via_csv(csv_dir)
        for key, val in csv_data.items():
            if key not in raw_data:
                raw_data[key] = val

    # 3. Process prices (Google Sheet como fuente primaria, pools como fallback)
    prices = process_prices(
        df_pools=raw_data.get("pools", pd.DataFrame()),
    )

    # 4. Process balances
    df_balances = process_balances(
        raw_data.get("balances_polygon", pd.DataFrame()),
        raw_data.get("balances_ethereum", pd.DataFrame()),
        df_wallets,
        prices,
    )
    if not df_balances.empty:
        df_balances.to_csv(PROCESSED_DIR / "balances.csv", index=False)
        log.info(f"Saved processed balances: {len(df_balances)} rows")

    # 5. Process outflows
    df_outflows = process_outflows(
        raw_data.get("outflows_polygon", pd.DataFrame()),
        raw_data.get("outflows_ethereum", pd.DataFrame()),
        df_wallets,
        prices,
    )
    if not df_outflows.empty:
        df_outflows.to_csv(PROCESSED_DIR / "outflows.csv", index=False)
        log.info(f"Saved processed outflows: {len(df_outflows)} rows")

    # 6. Compute summaries
    wallet_summary = compute_wallet_summary(df_balances, df_outflows, df_wallets)
    if not wallet_summary.empty:
        wallet_summary.to_csv(PROCESSED_DIR / "wallet_summary.csv", index=False)
        log.info(f"Saved wallet summary: {len(wallet_summary)} rows")

    global_metrics = compute_global_metrics(wallet_summary)
    with open(PROCESSED_DIR / "global_metrics.json", "w") as f:
        json.dump(global_metrics, f, indent=2, default=str)
    log.info(f"Saved global metrics")

    # 7. Save pools data as-is
    if "pools" in raw_data and not raw_data["pools"].empty:
        raw_data["pools"].to_csv(PROCESSED_DIR / "pools.csv", index=False)

    # 8. Combinar supply ETH + supply POL con columna network
    df_supply_eth = raw_data.get("supply_eth", pd.DataFrame())
    df_supply_pol = raw_data.get("supply_pol", pd.DataFrame())

    supply_frames = []
    if not df_supply_eth.empty:
        df_eth = df_supply_eth.copy()
        if "network" not in df_eth.columns:
            df_eth["network"] = "ethereum"
        supply_frames.append(df_eth)

    if not df_supply_pol.empty:
        df_pol = df_supply_pol.copy()
        if "network" not in df_pol.columns:
            df_pol["network"] = "polygon"
        supply_frames.append(df_pol)

    if supply_frames:
        df_supply_combined = pd.concat(supply_frames, ignore_index=True)
        df_supply_combined.to_csv(PROCESSED_DIR / "supply.csv", index=False)
        raw_data["supply"] = df_supply_combined
        log.info(f"Supply combinado: {len(df_supply_combined)} rows "
                 f"(ETH: {len(df_supply_eth)}, POL: {len(df_supply_pol)})")
    elif "supply" not in raw_data or raw_data["supply"].empty:
        raw_data["supply"] = pd.DataFrame()

    # 9b. Balances de contratos Arch (cuánto Arch token hold cada contrato)
    #     Filtra los raw balances por las direcciones de contratos conocidos
    all_contract_addrs = set(a.lower() for a in ARCH_CONTRACTS_POLYGON.keys())
    all_contract_addrs |= set(a.lower() for a in ARCH_CONTRACTS_ETHEREUM.keys())

    frames_contract = []
    for df_raw, network in [
        (raw_data.get("balances_polygon",  pd.DataFrame()), "polygon"),
        (raw_data.get("balances_ethereum", pd.DataFrame()), "ethereum"),
    ]:
        if df_raw.empty:
            continue
        df_c = df_raw[df_raw["wallet"].str.lower().isin(all_contract_addrs)].copy()
        if df_c.empty:
            continue
        df_c["network"] = network
        df_c["base_symbol"] = df_c["symbol"].map(SYMBOL_TO_BASE).fillna(df_c["symbol"])
        df_c["price_usd"]   = df_c["base_symbol"].map(prices).fillna(0)
        df_c["value_usd"]   = df_c["balance"] * df_c["price_usd"]
        frames_contract.append(df_c)

    if frames_contract:
        df_contracts = pd.concat(frames_contract, ignore_index=True)
        # Agregar nombre del contrato
        contract_names = {**ARCH_CONTRACTS_POLYGON, **ARCH_CONTRACTS_ETHEREUM}
        df_contracts["contract_name"] = df_contracts["wallet"].str.lower().map(
            {k.lower(): v for k, v in contract_names.items()}
        )
        df_contracts.to_csv(PROCESSED_DIR / "contract_balances.csv", index=False)
        total_val = df_contracts["value_usd"].sum()
        log.info(f"Contract balances: {len(df_contracts)} rows, ${total_val:,.0f} total en contratos Arch")

    # 9. Detect unknown wallets (on-chain but not in client CSV)
    #    Exclude the Arch internal AAGG contract (0xafb...) — already filtered via ARCHEMIST_TOKENS_POLYGON
    arch_internal = {
        "0xafb6e8331355fae99c8e8953bb4c6dc5d11e9f3c",  # Arch AAGG contract
        "0x530d10aec84fc3df7d5fd96e11357689352f69d6",  # Arch internal
    }

    df_unknown = compute_unknown_wallets(
        raw_data.get("balances_polygon", pd.DataFrame()),
        raw_data.get("outflows_polygon", pd.DataFrame()),
        df_wallets,
        prices,
        extra_exclude=arch_internal,
    )
    if not df_unknown.empty:
        df_unknown.to_csv(PROCESSED_DIR / "unknown_wallets.csv", index=False)
        log.info(f"Unknown wallets: {len(df_unknown)} wallets, "
                 f"${df_unknown['balance_usd'].sum():,.0f} remanente, "
                 f"${df_unknown['outflow_usd'].sum():,.0f} retirado")

    # 10. Unknown wallets → se agregan a wallet_summary como segment="sin_registro"
    #     para que el AUM total del overview incluya los tres segmentos:
    #     Preferente + Retail + Sin registro = Total
    if not df_unknown.empty and not wallet_summary.empty:
        unk_rows = pd.DataFrame({
            "wallet_address":     df_unknown["wallet_address"],
            "total_balance_usd":  df_unknown["balance_usd"].fillna(0),
            "total_outflow_usd":  df_unknown["outflow_usd"].fillna(0),
            "num_tokens":         pd.NA,
            "tokens_held":        df_unknown.get("tokens", pd.Series(dtype=str)),
            "network":            "polygon",
            "segment":            "sin_registro",   # ← tercer segmento, no retail
            "customer_name":      "",
            "email":              "",
            "num_outflow_events": df_unknown.get("num_txs", pd.Series(dtype=int)).fillna(0).astype(int),
            "last_outflow_date":  df_unknown.get("last_activity", pd.Series(dtype=str)),
            "outflow_tokens":     "",
        })
        def _status_unk(row):
            has_bal = row["total_balance_usd"] > DUST_THRESHOLD_USD
            has_out = row["total_outflow_usd"] > DUST_THRESHOLD_USD
            if has_bal and not has_out:   return "Sin movimiento"
            if has_bal and has_out:       return "Retiro parcial"
            if not has_bal and has_out:   return "Retirado completamente"
            return "Sin saldo (sin retiro detectado)"
        unk_rows["status"] = unk_rows.apply(_status_unk, axis=1)
        total_per = unk_rows["total_balance_usd"] + unk_rows["total_outflow_usd"]
        unk_rows["pct_withdrawn"] = (unk_rows["total_outflow_usd"] / total_per.replace(0, 1) * 100).round(2)

        wallet_summary = pd.concat([wallet_summary, unk_rows], ignore_index=True)
        wallet_summary = wallet_summary.sort_values("total_balance_usd", ascending=False)
        wallet_summary.to_csv(PROCESSED_DIR / "wallet_summary.csv", index=False)

        global_metrics = compute_global_metrics(wallet_summary)
        with open(PROCESSED_DIR / "global_metrics.json", "w") as f:
            json.dump(global_metrics, f, indent=2, default=str)
        log.info(f"Wallet summary: {len(unk_rows)} wallets sin registro agregadas como tercer segmento")

    # 11. Reconciliación por token: mismo origen de datos que el overview.
    #     remanente_usd + retirado_usd = histórico_usd (mismos datos que balances.csv / outflows.csv)
    df_recon = compute_reconciliation(
        raw_data.get("supply", pd.DataFrame()),
        pd.DataFrame(),   # df_balances_all_raw no se usa
        df_balances,
        prices,
        df_outflows=df_outflows,
    )
    if not df_recon.empty:
        df_recon.to_csv(PROCESSED_DIR / "reconciliation.csv", index=False)
        alertas = df_recon[df_recon["alerta"].str.startswith("⚠️")]
        if not alertas.empty:
            log.warning(f"⚠️ Reconciliación: {len(alertas)} tokens con datos inconsistentes")
        else:
            log.info(f"✓ Reconciliación OK: todos los tokens cuadran")

    log.info("=" * 60)
    log.info("  PIPELINE COMPLETE")
    log.info("=" * 60)

    return {
        "wallets": df_wallets,
        "balances": df_balances,
        "outflows": df_outflows,
        "wallet_summary": wallet_summary,
        "global_metrics": global_metrics,
        "prices": prices,
        "pools": raw_data.get("pools", pd.DataFrame()),
        "supply": raw_data.get("supply", pd.DataFrame()),
        "reconciliation": df_recon,
    }


if __name__ == "__main__":
    import os
    parser = argparse.ArgumentParser(description="Arch Finance Wind-Down Monitor Pipeline")
    parser.add_argument("--api-key", help="Dune Analytics API key (o usar env DUNE_API_KEY)")
    parser.add_argument("--csv-dir", help="Directory with pre-downloaded CSVs")
    parser.add_argument("--wallets", default=None, help="Path to wallets Excel file (.xlsx)")
    parser.add_argument("--pref-csv", default=None, help="Path to Preferentes CSV file")
    parser.add_argument("--retail-csv", default=None, help="Path to Retail CSV file")
    args = parser.parse_args()

    # API key: arg > env var
    api_key = args.api_key or os.environ.get("DUNE_API_KEY")

    if not api_key and not args.csv_dir:
        log.error("Provide either --api-key / DUNE_API_KEY env var, or --csv-dir")
        sys.exit(1)

    result = run_pipeline(
        api_key=api_key,
        csv_dir=args.csv_dir,
        wallets_path=args.wallets,
        preferentes_csv=args.pref_csv,
        retail_csv=args.retail_csv,
    )

    # Print summary
    m = result["global_metrics"]
    if m:
        print("\n" + "=" * 50)
        print("  RESUMEN EJECUTIVO")
        print("=" * 50)
        print(f"  Total remanente:   ${m.get('total_remaining_usd', 0):>14,.2f}")
        print(f"  Total retirado:    ${m.get('total_withdrawn_usd', 0):>14,.2f}")
        print(f"  % remanente:       {m.get('pct_remaining', 0):>13.1f}%")
        print(f"  % retirado:        {m.get('pct_withdrawn', 0):>13.1f}%")
        print(f"  Wallets con saldo: {m.get('wallets_with_balance', 0):>14}")
        print(f"  Wallets vacias:    {m.get('wallets_without_balance', 0):>14}")
        print("=" * 50)
