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
"""


# =========================================================================
# Q2: CURRENT BALANCES - ETHEREUM
# Same logic for ETH chain (fewer tokens, fewer wallets)
# =========================================================================
def query_balances_ethereum() -> str:
    tokens_values = _token_values_clause(TOKENS_ETHEREUM)
    return f"""
-- Q2: Current balances of Arch tokens on Ethereum
-- Tokens: WEB3, CHAIN, ACAI, ADDY, AEDY, ABDY, CHAIN_SET, WEB3_SET
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
"""


# =========================================================================
# Q3: OUTFLOWS - POLYGON (rolling 7-day window, append mode)
# =========================================================================
def query_outflows_polygon() -> str:
    tokens_values = _token_values_clause(ALL_TOKENS_POLYGON)
    return f"""
-- Q3: Outgoing transfers of Arch tokens on Polygon — last 7 days
-- Pipeline appends results to existing CSV, deduplicating by (tx_hash, wallet_from, symbol)
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
WHERE t.evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
  AND t."from" != 0x0000000000000000000000000000000000000000
  AND CAST(t.value AS DOUBLE) / POWER(10, a.decimals) > 0.0001
"""


# =========================================================================
# Q4: OUTFLOWS - ETHEREUM (rolling 7-day window, append mode)
# =========================================================================
def query_outflows_ethereum() -> str:
    tokens_values = _token_values_clause(TOKENS_ETHEREUM)
    return f"""
-- Q4: Outgoing transfers of Arch tokens on Ethereum — last 7 days
-- Pipeline appends results to existing CSV, deduplicating by (tx_hash, wallet_from, symbol)
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
WHERE t.evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
  AND t."from" != 0x0000000000000000000000000000000000000000
  AND CAST(t.value AS DOUBLE) / POWER(10, a.decimals) > 0.0001
"""


# =========================================================================
# Q5: USDC INFLOWS to client wallets (confirms redemption received)
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
"""


# =========================================================================
# Q6: SUPPLY - ETHEREUM
# Base tokens + SET versions → last 5 days
# CHAIN address confirmed: 0x89c53b02558e4d1c24b9bf3bed1279871187ef0b
# =========================================================================
def query_supply_eth() -> str:
    """Supply diario en Ethereum — tokens base + SET (query 7370113)."""
    return """
WITH
eth_tokens AS (
    SELECT * FROM (VALUES
        (0x8f0d5660929ca6ac394c5c41f59497629b1dbc23, 'WEB3',      18),
        (0x89c53b02558e4d1c24b9bf3bed1279871187ef0b, 'CHAIN',     18),
        (0xd1ce69b4bdd3dda553ea55a2a57c21c65190f3d5, 'ACAI',      18),
        (0xe15a66b7b8e385caa6f69fd0d55984b96d7263cf, 'ADDY',      18),
        (0x103bb3ebc6f61b3db2d6e01e54ef7d9899a2e16b, 'AEDY',      18),
        (0xde2925d582fc8711a0e93271c12615bdd043ed1c, 'ABDY',      18),
        (0x0d20e86abab680c038ac8bbdc1446585e67f8951, 'CHAIN_SET', 18),
        (0xe8e8486228753e01dbc222da262aa706bd67e601, 'WEB3_SET',  18)
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
),
eth_supply AS (
    SELECT
        day,
        label,
        CAST(contract_address AS VARCHAR) AS contract_address,
        'ethereum' AS network,
        SUM(daily_change) OVER (PARTITION BY label ORDER BY day) AS supply
    FROM eth_daily
)
SELECT * FROM eth_supply
WHERE day >= CURRENT_DATE - INTERVAL '5' DAY
"""


# =========================================================================
# Q7: SUPPLY - POLYGON
# All tokens: base _PROD, portfolios, SET, ABDY_V1 → last 5 days
# =========================================================================
def query_supply_pol() -> str:
    """Supply diario en Polygon — todos los tokens (query 6963145)."""
    return """
WITH tokens AS (
  SELECT contract_address, label
  FROM (VALUES
    (0xc4ea087fc2cb3a1d9ff86c676f03abe4f3ee906f, 'WEB3'),
    (0x70a13201df2364b634cb5aac8d735db3a654b30c, 'CHAIN'),
    (0x9f5c845a178dfcb9abe1e9d3649269826ce43901, 'ACAI'),
    (0xab1b1680f6037006e337764547fb82d17606c187, 'ADDY'),
    (0x027af1e12a5869ed329be4c05617ad528e997d5a, 'AEDY'),
    (0xef7b6cd33afafc36379289b7accae95116e27c88, 'ABDY'),
    (0xde2925d582fc8711a0e93271c12615bdd043ed1c, 'ABDY_V1'),
    (0xafb6e8331355fae99c8e8953bb4c6dc5d11e9f3c, 'AAGG'),
    (0xa5a979aa7f55798e99f91abe815c114a09164beb, 'AMOD'),
    (0xf401e2c1ce8f252947b60bfb92578f84217a1545, 'ABAL'),
    (0x6ca9c8914a14d63a6700556127d09e7721ff7d3b, 'AP60'),
    (0x9a41e03fef7f16f552c6fba37ffa7590fb1ec0c4, 'CHAIN_SET'),
    (0xbcd2c5c78000504efbc1ce6489dfcac71835406a, 'WEB3_SET')
  ) AS t(contract_address, label)
),
deltas AS (
  SELECT
    evt.contract_address,
    date_trunc('day', evt.evt_block_time) AS day,
    CASE
      WHEN evt."from" = 0x0000000000000000000000000000000000000000 THEN  CAST(evt.value AS DECIMAL(38,0))
      WHEN evt."to"   = 0x0000000000000000000000000000000000000000 THEN -CAST(evt.value AS DECIMAL(38,0))
      ELSE 0
    END AS delta_raw
  FROM erc20_polygon.evt_transfer evt
  JOIN tokens ON evt.contract_address = tokens.contract_address
),
daily AS (
  SELECT contract_address, day, SUM(delta_raw) AS delta_raw
  FROM deltas
  GROUP BY contract_address, day
),
running AS (
  SELECT
    contract_address,
    CAST(day AS DATE) AS day,
    SUM(delta_raw) OVER (
      PARTITION BY contract_address
      ORDER BY day
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS supply_raw
  FROM daily
)
SELECT
  r.day,
  t.label,
  CAST(r.contract_address AS VARCHAR) AS contract_address,
  'polygon' AS network,
  CAST(r.supply_raw AS DOUBLE) / 1e18 AS supply
FROM running r
JOIN tokens t ON r.contract_address = t.contract_address
WHERE r.day >= CURRENT_DATE - INTERVAL '5' DAY
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
        ("Q1: Balances Polygon      (6993335)", query_balances_polygon()),
        ("Q2: Balances Ethereum     (6993356)", query_balances_ethereum()),
        ("Q3: Outflows Polygon      (6993341)", query_outflows_polygon()),
        ("Q4: Outflows Ethereum     (6993368)", query_outflows_ethereum()),
        ("Q5: USDC Inflows Polygon           ", query_usdc_inflows_polygon()),
        ("Q6: Supply ETH            (7370113)", query_supply_eth()),
        ("Q7: Supply POL            (6963145)", query_supply_pol()),
    ]
    for name, sql in queries:
        print(f"\n{'='*70}")
        print(f"  {name}")
        print(f"{'='*70}")
        print(sql)
        print()


if __name__ == "__main__":
    print_all_queries()
