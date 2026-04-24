"""
Arch Finance - Wind-Down Monitor
Dune SQL Queries (DuneSQL / Trino syntax)

Each query is a standalone function that returns SQL text.
Queries are designed to return ALL data (no wallet filtering) so
filtering happens in Python after download. This avoids hitting
Dune's query parameter limits with 2900+ wallets.

NOTE: In DuneSQL, addresses are varbinary. Use 0x... literals directly.
"""

from config import (
    ALL_TOKENS_POLYGON, TOKENS_ETHEREUM,
    ARCHEMIST_TOKENS_POLYGON, CUTOFF_DATE,
)


def _token_values_clause(tokens_dict: dict, alias: str = "t") -> str:
    """Generate a VALUES clause for token addresses."""
    rows = []
    for addr, info in tokens_dict.items():
        sym = info["symbol"]
        dec = info.get("decimals", 18)
        rows.append(f"        ({addr}, '{sym}', {dec})")
    return f"    (VALUES\n" + ",\n".join(rows) + f"\n    ) AS {alias}(token_address, symbol, decimals)"


# =========================================================================
# Q1: CURRENT BALANCES - POLYGON
# All holders of any Arch token on Polygon with balance > dust
# =========================================================================
def query_balances_polygon() -> str:
    tokens_values = _token_values_clause(ALL_TOKENS_POLYGON)
    return f"""
-- Q1: Current balances of all Arch tokens on Polygon
-- Returns: wallet, token_address, symbol, decimals, balance
WITH arch_tokens AS (
    SELECT * FROM
{tokens_values}
),
inflows AS (
    SELECT
        t."to" AS wallet,
        t.contract_address AS token_address,
        SUM(CAST(t.value AS DOUBLE)) AS amount
    FROM erc20_polygon.evt_Transfer t
    INNER JOIN arch_tokens a ON t.contract_address = a.token_address
    WHERE t."to" != 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
),
outflows AS (
    SELECT
        t."from" AS wallet,
        t.contract_address AS token_address,
        SUM(CAST(t.value AS DOUBLE)) AS amount
    FROM erc20_polygon.evt_Transfer t
    INNER JOIN arch_tokens a ON t.contract_address = a.token_address
    WHERE t."from" != 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
),
net_balances AS (
    SELECT
        COALESCE(i.wallet, o.wallet) AS wallet,
        COALESCE(i.token_address, o.token_address) AS token_address,
        (COALESCE(i.amount, 0) - COALESCE(o.amount, 0)) AS raw_balance
    FROM inflows i
    FULL OUTER JOIN outflows o
        ON i.wallet = o.wallet AND i.token_address = o.token_address
)
SELECT
    CAST(nb.wallet AS VARCHAR) AS wallet,
    CAST(nb.token_address AS VARCHAR) AS token_address,
    a.symbol,
    a.decimals,
    nb.raw_balance / POWER(10, a.decimals) AS balance
FROM net_balances nb
JOIN arch_tokens a ON nb.token_address = a.token_address
WHERE nb.raw_balance / POWER(10, a.decimals) > 0.0001
ORDER BY balance DESC
"""


# =========================================================================
# Q2: CURRENT BALANCES - ETHEREUM
# Same logic for ETH chain (fewer tokens, fewer wallets)
# =========================================================================
def query_balances_ethereum() -> str:
    tokens_values = _token_values_clause(TOKENS_ETHEREUM)
    return f"""
-- Q2: Current balances of Arch tokens on Ethereum
WITH arch_tokens AS (
    SELECT * FROM
{tokens_values}
),
inflows AS (
    SELECT
        t."to" AS wallet,
        t.contract_address AS token_address,
        SUM(CAST(t.value AS DOUBLE)) AS amount
    FROM erc20_ethereum.evt_Transfer t
    INNER JOIN arch_tokens a ON t.contract_address = a.token_address
    WHERE t."to" != 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
),
outflows AS (
    SELECT
        t."from" AS wallet,
        t.contract_address AS token_address,
        SUM(CAST(t.value AS DOUBLE)) AS amount
    FROM erc20_ethereum.evt_Transfer t
    INNER JOIN arch_tokens a ON t.contract_address = a.token_address
    WHERE t."from" != 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
),
net_balances AS (
    SELECT
        COALESCE(i.wallet, o.wallet) AS wallet,
        COALESCE(i.token_address, o.token_address) AS token_address,
        (COALESCE(i.amount, 0) - COALESCE(o.amount, 0)) AS raw_balance
    FROM inflows i
    FULL OUTER JOIN outflows o
        ON i.wallet = o.wallet AND i.token_address = o.token_address
)
SELECT
    CAST(nb.wallet AS VARCHAR) AS wallet,
    CAST(nb.token_address AS VARCHAR) AS token_address,
    a.symbol,
    a.decimals,
    nb.raw_balance / POWER(10, a.decimals) AS balance
FROM net_balances nb
JOIN arch_tokens a ON nb.token_address = a.token_address
WHERE nb.raw_balance / POWER(10, a.decimals) > 0.0001
ORDER BY balance DESC
"""


# =========================================================================
# Q3: OUTFLOWS - POLYGON (since cutoff date)
# All transfers of Arch tokens FROM any address since the cutoff
# =========================================================================
def query_outflows_polygon(cutoff: str = CUTOFF_DATE) -> str:
    tokens_values = _token_values_clause(ALL_TOKENS_POLYGON)
    return f"""
-- Q3: All outgoing transfers of Arch tokens on Polygon since {cutoff}
-- We capture ALL transfers (not just to Arch contracts) to detect
-- any pattern: redeems, swaps, transfers, etc.
WITH arch_tokens AS (
    SELECT * FROM
{tokens_values}
)
SELECT
    CAST(t."from" AS VARCHAR) AS wallet_from,
    CAST(t."to" AS VARCHAR) AS wallet_to,
    CAST(t.contract_address AS VARCHAR) AS token_address,
    a.symbol,
    CAST(t.value AS DOUBLE) / POWER(10, a.decimals) AS amount,
    t.evt_block_time AS block_time,
    CAST(t.evt_tx_hash AS VARCHAR) AS tx_hash
FROM erc20_polygon.evt_Transfer t
INNER JOIN arch_tokens a ON t.contract_address = a.token_address
WHERE t.evt_block_time >= TIMESTAMP '{cutoff} 00:00:00'
  AND t."from" != 0x0000000000000000000000000000000000000000
  AND CAST(t.value AS DOUBLE) / POWER(10, a.decimals) > 0.0001
ORDER BY t.evt_block_time DESC
"""


# =========================================================================
# Q4: OUTFLOWS - ETHEREUM (since cutoff date)
# =========================================================================
def query_outflows_ethereum(cutoff: str = CUTOFF_DATE) -> str:
    tokens_values = _token_values_clause(TOKENS_ETHEREUM)
    return f"""
-- Q4: All outgoing transfers of Arch tokens on Ethereum since {cutoff}
WITH arch_tokens AS (
    SELECT * FROM
{tokens_values}
)
SELECT
    CAST(t."from" AS VARCHAR) AS wallet_from,
    CAST(t."to" AS VARCHAR) AS wallet_to,
    CAST(t.contract_address AS VARCHAR) AS token_address,
    a.symbol,
    CAST(t.value AS DOUBLE) / POWER(10, a.decimals) AS amount,
    t.evt_block_time AS block_time,
    CAST(t.evt_tx_hash AS VARCHAR) AS tx_hash
FROM erc20_ethereum.evt_Transfer t
INNER JOIN arch_tokens a ON t.contract_address = a.token_address
WHERE t.evt_block_time >= TIMESTAMP '{cutoff} 00:00:00'
  AND t."from" != 0x0000000000000000000000000000000000000000
  AND CAST(t.value AS DOUBLE) / POWER(10, a.decimals) > 0.0001
ORDER BY t.evt_block_time DESC
"""


# =========================================================================
# Q5: USDC INFLOWS to client wallets (confirms redemption received)
# Optional: helps validate that an outflow of Arch token resulted in USDC
# =========================================================================
def query_usdc_inflows_polygon(cutoff: str = CUTOFF_DATE) -> str:
    return f"""
-- Q5: USDC received by addresses that also hold/held Arch tokens (Polygon)
-- Useful to cross-validate: outflow of Arch token → inflow of USDC
SELECT
    CAST(t."to" AS VARCHAR) AS wallet,
    CAST(t."from" AS VARCHAR) AS source,
    CAST(t.contract_address AS VARCHAR) AS usdc_contract,
    CASE
        WHEN t.contract_address = 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359 THEN 'USDC (native)'
        WHEN t.contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 THEN 'USDC.e (bridged)'
    END AS usdc_type,
    CAST(t.value AS DOUBLE) / 1e6 AS amount_usdc,
    t.evt_block_time AS block_time,
    CAST(t.evt_tx_hash AS VARCHAR) AS tx_hash
FROM erc20_polygon.evt_Transfer t
WHERE t.contract_address IN (
    0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359,
    0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
)
AND t.evt_block_time >= TIMESTAMP '{cutoff} 00:00:00'
AND CAST(t.value AS DOUBLE) / 1e6 > 0.01
ORDER BY t.evt_block_time DESC
"""


# =========================================================================
# Q6: SUPPLY CORRECTO
# Base tokens (WEB3, CHAIN, ACAI, ADDY, AEDY, ABDY) → supply en ETHEREUM
# Portfolio tokens (AAGG, AMOD, ABAL)               → supply en POLYGON
# =========================================================================
def query_supply_eth() -> str:
    """Supply diario en Ethereum — solo tokens base (query 7370113)."""
    return """
WITH
eth_tokens AS (
    SELECT * FROM (VALUES
        (0x8f0d5660929ca6ac394c5c41f59497629b1dbc23, 'WEB3',  18),
        (0x89c53b02558e4d1c24cb5aac8d735db3a654b30b, 'CHAIN', 18),
        (0xd1ce69b4bdd3dda553ea55a2a57c21c65190f3d5, 'ACAI',  18),
        (0xe15a66b7b8e385caa6f69fd0d55984b96d7263cf, 'ADDY',  18),
        (0x103bb3ebc6f61b3db2d6e01e54ef7d9899a2e16b, 'AEDY',  18),
        (0xde2925d582fc8711a0e93271c12615bdd043ed1c, 'ABDY',  18)
    ) AS t(contract_address, label, decimals)
),
eth_daily AS (
    SELECT
        date_trunc('day', t.evt_block_time) AS day,
        a.label,
        a.contract_address,
        SUM(
            CASE
                WHEN t."to"   = 0x0000000000000000000000000000000000000000 THEN -CAST(t.value AS DOUBLE)
                WHEN t."from" = 0x0000000000000000000000000000000000000000 THEN  CAST(t.value AS DOUBLE)
                ELSE 0
            END
        ) / POWER(10, a.decimals) AS daily_change
    FROM erc20_ethereum.evt_Transfer t
    INNER JOIN eth_tokens a ON t.contract_address = a.contract_address
    WHERE t."from" = 0x0000000000000000000000000000000000000000
       OR t."to"   = 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2, 3, a.decimals
)
SELECT
    day,
    label,
    CAST(contract_address AS VARCHAR) AS contract_address,
    'ethereum' AS network,
    SUM(daily_change) OVER (PARTITION BY label ORDER BY day) AS supply
FROM eth_daily
WHERE day >= CURRENT_DATE - INTERVAL '5' DAY
ORDER BY label, day
"""


# Alias para compatibilidad
def query_supply_correct() -> str:
    return query_supply_eth()


# =========================================================================
# UTILITY: Print all queries for manual use in Dune UI
# =========================================================================
def print_all_queries():
    """Print all queries for copy-paste into Dune UI."""
    queries = [
        ("Q1: Balances Polygon", query_balances_polygon()),
        ("Q2: Balances Ethereum", query_balances_ethereum()),
        ("Q3: Outflows Polygon", query_outflows_polygon()),
        ("Q4: Outflows Ethereum", query_outflows_ethereum()),
        ("Q5: USDC Inflows Polygon", query_usdc_inflows_polygon()),
        ("Q6: Supply Correcto (ETH base + POL portfolios)", query_supply_correct()),
    ]
    for name, sql in queries:
        print(f"\n{'='*70}")
        print(f"  {name}")
        print(f"{'='*70}")
        print(sql)
        print()


if __name__ == "__main__":
    print_all_queries()
