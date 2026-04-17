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
    DUNE_API_BASE, DUNE_QUERY_PRICES, DUNE_QUERY_SUPPLY, DUNE_QUERY_POOLS,
    DUNE_QUERY_BALANCES_POLYGON, DUNE_QUERY_OUTFLOWS_POLYGON,
    DUNE_QUERY_BALANCES_ETHEREUM, DUNE_QUERY_OUTFLOWS_ETHEREUM,
    VAULT_POSITIONS,
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

    def run_query_id(self, query_id: int, params: dict = None) -> pd.DataFrame:
        """Execute a saved query by ID and return results."""
        log.info(f"Executing Dune query {query_id}...")
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
def extract_via_api(dune: DuneClient) -> dict:
    """Extract all data from Dune API using saved query IDs. Returns dict of DataFrames."""
    results = {}

    queries = [
        ("prices",             DUNE_QUERY_PRICES),
        ("supply",             DUNE_QUERY_SUPPLY),
        ("pools",              DUNE_QUERY_POOLS),
        ("balances_polygon",   DUNE_QUERY_BALANCES_POLYGON),
        ("outflows_polygon",   DUNE_QUERY_OUTFLOWS_POLYGON),
        ("balances_ethereum",  DUNE_QUERY_BALANCES_ETHEREUM),
        ("outflows_ethereum",  DUNE_QUERY_OUTFLOWS_ETHEREUM),
    ]

    for name, query_id in queries:
        try:
            log.info(f"--- Executing query {query_id} ({name}) ---")
            results[name] = dune.run_query_id(query_id)
            log.info(f"  ✓ {name}: {len(results[name])} rows")
        except Exception as e:
            log.error(f"  ✗ Failed {name} (query {query_id}): {e}")
            results[name] = pd.DataFrame()

    # NAV prices for portfolio vault tokens (AAGG, AMOD, ABAL, AP60)
    # Requires creating a query in Dune UI and setting DUNE_QUERY_VAULT_NAV in config.py
    # SQL to use (create at dune.com/queries/new):
    #
    #   SELECT symbol, contract_address, price AS nav_usd
    #   FROM prices.usd
    #   WHERE blockchain = 'polygon'
    #   AND contract_address IN (
    #     0xafb6e8331355fae99c8e8953bb4c6dc5d11e9f3c,
    #     0xa5a979aa7f55798e99f91abe815c114a09164beb,
    #     0xf401e2c1ce8f252947b60bfb92578f84217a1545,
    #     0x6ca9c891ba6a034d7553a97a7b7a55c3ce04b15c
    #   )
    #   AND minute >= NOW() - interval '2 hours'
    #   ORDER BY minute DESC
    #   LIMIT 10
    #
    if DUNE_QUERY_VAULT_NAV:
        try:
            log.info(f"--- Fetching vault NAV prices (query {DUNE_QUERY_VAULT_NAV}) ---")
            results["vault_nav"] = dune.run_query_id(DUNE_QUERY_VAULT_NAV)
            log.info(f"  ✓ vault_nav: {len(results['vault_nav'])} rows")
        except Exception as e:
            log.warning(f"  ✗ vault_nav failed: {e}")
            results["vault_nav"] = pd.DataFrame()
    else:
        log.info("DUNE_QUERY_VAULT_NAV not set — vault NAV will be computed from hardcoded VAULT_POSITIONS")

    # Save all as raw CSVs
    for name, df in results.items():
        if not df.empty:
            path = RAW_DIR / f"{name}.csv"
            df.to_csv(path, index=False)
            log.info(f"  Saved {name} -> {path} ({len(df)} rows)")

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
def process_prices(df_prices: pd.DataFrame, df_pools: pd.DataFrame = None, df_vault_nav: pd.DataFrame = None) -> dict:
    """
    Process prices into a symbol -> USD price dict.

    Sources (in order of priority):
    1. Pools data (query 3591853): most reliable for the 6 Archemist tokens.
    2. Price query (6963204): supplement for missing tokens.
    3. Vault NAV query (DUNE_QUERY_VAULT_NAV): NAV for AAGG, AMOD, ABAL, AP60.
    4. SYMBOL_TO_BASE propagation: extends to _PROD, _V1, _SET variants.
    """
    prices = {}

    # --- Source 1: pools data (most reliable) ---
    if df_pools is not None and not df_pools.empty:
        for _, row in df_pools.iterrows():
            token = str(row.get("token", "")).strip().upper()
            if not token or token == "USDC":
                continue
            try:
                price = float(row.get("price", 0) or 0)
            except (ValueError, TypeError):
                price = 0
            if price > 0:
                prices[token] = price
        log.info(f"Prices from pools: {prices}")

    # --- Source 2: price query (supplement — only fill gaps not covered by pools) ---
    # Dune query 6963204 returns historical pricePerShare data; columns vary:
    #   'contract_address' or 'address', and 'pricePerShare_in_usdc' or 'precio' or 'price'
    if df_prices is not None and not df_prices.empty:
        addr_to_sym = {addr.lower(): info["symbol"] for addr, info in ARCHEMIST_TOKENS_POLYGON.items()}
        # normalize column names
        col_addr  = next((c for c in ["address", "contract_address"] if c in df_prices.columns), None)
        col_price = next((c for c in ["precio", "pricePerShare_in_usdc", "price", "price_usd"] if c in df_prices.columns), None)
        if col_addr and col_price:
            # Take the most recent entry per address (last row wins or use max)
            for addr_raw, grp in df_prices.groupby(col_addr):
                addr = str(addr_raw).lower()
                if addr not in addr_to_sym:
                    continue
                sym = addr_to_sym[addr]
                if sym in prices:
                    continue  # already have from pools — don't override
                try:
                    price = float(grp[col_price].iloc[-1] or 0)
                except (ValueError, TypeError):
                    price = 0
                if price > 0:
                    prices[sym] = price
        log.info(f"Prices after price query supplement: {prices}")

    if not prices:
        log.warning("No price data found in pools or price query. All USD values will be $0.")
        return {}

    # --- Source 3: vault NAV prices computed from hardcoded VAULT_POSITIONS ---
    # NAV = sum(units_i × price_i) for each component token in the portfolio vault.
    # Positions sourced from Polygonscan getPositions() — see config.VAULT_POSITIONS.
    for vault_sym, components in VAULT_POSITIONS.items():
        nav = sum(
            units * prices.get(comp_sym, 0)
            for comp_sym, units in components.items()
        )
        if nav > 0:
            prices[vault_sym] = nav
            log.info(f"  Vault NAV computed: {vault_sym} = ${nav:.4f} "
                     f"(components: { {c: round(units * prices.get(c, 0), 4) for c, units in components.items()} })")

    # --- Source 4: propagate to variants via SYMBOL_TO_BASE ---
    # e.g. WEB3_PROD → WEB3 price, ABDY_V1 → ABDY price, CHAIN_SET → CHAIN price
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

    # Merge (outer so fully-withdrawn wallets still appear from out_agg side)
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
# 5b. UNKNOWN WALLET DETECTION
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

    # 3. Process prices (pools → price query → vault NAV)
    prices = process_prices(
        raw_data.get("prices", pd.DataFrame()),
        raw_data.get("pools", pd.DataFrame()),
        raw_data.get("vault_nav", pd.DataFrame()),
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

    # 8. Save supply data
    if "supply" in raw_data and not raw_data["supply"].empty:
        raw_data["supply"].to_csv(PROCESSED_DIR / "supply.csv", index=False)

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

    # 10. Merge unknown wallets into wallet_summary as segment="retail"
    #     so their totals appear in all dashboard metrics
    if not df_unknown.empty and not wallet_summary.empty:
        unk_rows = pd.DataFrame({
            "wallet_address":    df_unknown["wallet_address"],
            "total_balance_usd": df_unknown["balance_usd"].fillna(0),
            "total_outflow_usd": df_unknown["outflow_usd"].fillna(0),
            "num_tokens":        pd.NA,
            "tokens_held":       df_unknown.get("tokens", pd.Series(dtype=str)),
            "network":           "polygon",
            "segment":           "retail",
            "customer_name":     "",
            "email":             "",
            "num_outflow_events": df_unknown.get("num_txs", pd.Series(dtype=int)).fillna(0).astype(int),
            "last_outflow_date": df_unknown.get("last_activity", pd.Series(dtype=str)),
            "outflow_tokens":    "",
        })
        # Classify status
        def _status(row):
            has_bal = row["total_balance_usd"] > DUST_THRESHOLD_USD
            has_out = row["total_outflow_usd"] > DUST_THRESHOLD_USD
            if has_bal and not has_out:   return "Sin movimiento"
            if has_bal and has_out:       return "Retiro parcial"
            if not has_bal and has_out:   return "Retirado completamente"
            return "Sin saldo (sin retiro detectado)"
        unk_rows["status"] = unk_rows.apply(_status, axis=1)
        total_per = unk_rows["total_balance_usd"] + unk_rows["total_outflow_usd"]
        unk_rows["pct_withdrawn"] = (unk_rows["total_outflow_usd"] / total_per.replace(0, 1) * 100).round(2)

        wallet_summary = pd.concat([wallet_summary, unk_rows], ignore_index=True)
        wallet_summary = wallet_summary.sort_values("total_balance_usd", ascending=False)
        wallet_summary.to_csv(PROCESSED_DIR / "wallet_summary.csv", index=False)

        # Recompute global metrics with unknowns included
        global_metrics = compute_global_metrics(wallet_summary)
        with open(PROCESSED_DIR / "global_metrics.json", "w") as f:
            json.dump(global_metrics, f, indent=2, default=str)
        log.info(f"Wallet summary updated with {len(unk_rows)} unknown wallets merged into retail")

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