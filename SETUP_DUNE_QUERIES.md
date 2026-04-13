# Configurar Queries de Dune

Las queries de balances y outflows necesitan estar guardadas como queries en Dune
para que el pipeline pueda ejecutarlas con tu plan actual.

## Pasos

1. Ve a https://dune.com/queries/new
2. Pega el SQL de cada query abajo
3. Haz click en "Save" → anota el ID que aparece en la URL (ej: dune.com/queries/**1234567**)
4. Edita el archivo `config.py` y agrega los IDs donde se indica
5. Re-ejecuta el pipeline

---

## Query 1: Balances Polygon

Guárdala como: `arch_balances_polygon`

```sql
WITH arch_tokens AS (
    SELECT * FROM (VALUES
        (0xC68140cdf17566F8AD43db8487d6600196d79176, 'WEB3', 18),
        (0xC770C918332522aC66306a83989ba1b5B807c1ae, 'CHAIN', 18),
        (0x2C0c8A17a58d37F0cA75cf9482307a8c6043d252, 'ACAI', 18),
        (0xCFA916cCeb4a32d727210B21b7A23FBCcC5c4e7D, 'ABDY', 18),
        (0x774Aac3a6F0Da70D57621dE098cf4d2Ef77bf1A5, 'ADDY', 18),
        (0x578934dF6e0f1d525Ee5cE3397A7c4C554945efA, 'AEDY', 18),
        (0xc4ea087fc2cb3a1d9ff86c676f03abe4f3ee906f, 'WEB3_PROD', 18),
        (0x70a13201df2364b634cb5aac8d735db3a654b30c, 'CHAIN_PROD', 18),
        (0x9f5c845a178dfcb9abe1e9d3649269826ce43901, 'ACAI_PROD', 18),
        (0xab1b1680f6037006e337764547fb82d17606c187, 'ADDY_PROD', 18),
        (0x027af1e12a5869ed329be4c05617ad528e997d5a, 'AEDY_PROD', 18),
        (0xef7b6cd33afafc36379289b7accae95116e27c88, 'ABDY_PROD', 18),
        (0xafb6e8331355fae99c8e8953bb4c6dc5d11e9f3c, 'AAGG', 18),
        (0xa5a979aa7f55798e99f91abe815c114a09164beb, 'AMOD', 18),
        (0xf401e2c1ce8f252947b60bfb92578f84217a1545, 'ABAL', 18),
        (0x6ca9c8914a14d63a6700556127d09e7721ff7d3b, 'AP60', 18),
        (0x9a41e03fef7f16f552c6fba37ffa7590fb1ec0c4, 'CHAIN_SET', 18),
        (0xbcd2c5c78000504efbc1ce6489dfcac71835406a, 'WEB3_SET', 18),
        (0xde2925d582fc8711a0e93271c12615bdd043ed1c, 'ABDY_V1', 18)
    ) AS t(token_address, symbol, decimals)
),
inflows AS (
    SELECT t."to" AS wallet, t.contract_address AS token_address,
           SUM(CAST(t.value AS DOUBLE)) AS amount
    FROM erc20_polygon.evt_Transfer t
    INNER JOIN arch_tokens a ON t.contract_address = a.token_address
    WHERE t."to" != 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
),
outflows AS (
    SELECT t."from" AS wallet, t.contract_address AS token_address,
           SUM(CAST(t.value AS DOUBLE)) AS amount
    FROM erc20_polygon.evt_Transfer t
    INNER JOIN arch_tokens a ON t.contract_address = a.token_address
    WHERE t."from" != 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
),
net_balances AS (
    SELECT COALESCE(i.wallet, o.wallet) AS wallet,
           COALESCE(i.token_address, o.token_address) AS token_address,
           (COALESCE(i.amount, 0) - COALESCE(o.amount, 0)) AS raw_balance
    FROM inflows i
    FULL OUTER JOIN outflows o ON i.wallet = o.wallet AND i.token_address = o.token_address
)
SELECT CAST(nb.wallet AS VARCHAR) AS wallet,
       CAST(nb.token_address AS VARCHAR) AS token_address,
       a.symbol, a.decimals,
       nb.raw_balance / POWER(10, a.decimals) AS balance
FROM net_balances nb
JOIN arch_tokens a ON nb.token_address = a.token_address
WHERE nb.raw_balance / POWER(10, a.decimals) > 0.0001
ORDER BY balance DESC
```

---

## Query 2: Outflows Polygon (desde 2026-04-01)

Guárdala como: `arch_outflows_polygon`

```sql
WITH arch_tokens AS (
    SELECT * FROM (VALUES
        (0xC68140cdf17566F8AD43db8487d6600196d79176, 'WEB3', 18),
        (0xC770C918332522aC66306a83989ba1b5B807c1ae, 'CHAIN', 18),
        (0x2C0c8A17a58d37F0cA75cf9482307a8c6043d252, 'ACAI', 18),
        (0xCFA916cCeb4a32d727210B21b7A23FBCcC5c4e7D, 'ABDY', 18),
        (0x774Aac3a6F0Da70D57621dE098cf4d2Ef77bf1A5, 'ADDY', 18),
        (0x578934dF6e0f1d525Ee5cE3397A7c4C554945efA, 'AEDY', 18),
        (0xc4ea087fc2cb3a1d9ff86c676f03abe4f3ee906f, 'WEB3_PROD', 18),
        (0x70a13201df2364b634cb5aac8d735db3a654b30c, 'CHAIN_PROD', 18),
        (0x9f5c845a178dfcb9abe1e9d3649269826ce43901, 'ACAI_PROD', 18),
        (0xab1b1680f6037006e337764547fb82d17606c187, 'ADDY_PROD', 18),
        (0x027af1e12a5869ed329be4c05617ad528e997d5a, 'AEDY_PROD', 18),
        (0xef7b6cd33afafc36379289b7accae95116e27c88, 'ABDY_PROD', 18),
        (0xafb6e8331355fae99c8e8953bb4c6dc5d11e9f3c, 'AAGG', 18),
        (0xa5a979aa7f55798e99f91abe815c114a09164beb, 'AMOD', 18),
        (0xf401e2c1ce8f252947b60bfb92578f84217a1545, 'ABAL', 18),
        (0x6ca9c8914a14d63a6700556127d09e7721ff7d3b, 'AP60', 18),
        (0x9a41e03fef7f16f552c6fba37ffa7590fb1ec0c4, 'CHAIN_SET', 18),
        (0xbcd2c5c78000504efbc1ce6489dfcac71835406a, 'WEB3_SET', 18),
        (0xde2925d582fc8711a0e93271c12615bdd043ed1c, 'ABDY_V1', 18)
    ) AS t(token_address, symbol, decimals)
)
SELECT CAST(t."from" AS VARCHAR) AS wallet_from,
       CAST(t."to" AS VARCHAR) AS wallet_to,
       CAST(t.contract_address AS VARCHAR) AS token_address,
       a.symbol,
       CAST(t.value AS DOUBLE) / POWER(10, a.decimals) AS amount,
       t.evt_block_time AS block_time,
       CAST(t.evt_tx_hash AS VARCHAR) AS tx_hash
FROM erc20_polygon.evt_Transfer t
INNER JOIN arch_tokens a ON t.contract_address = a.token_address
WHERE t.evt_block_time >= TIMESTAMP '2026-04-01 00:00:00'
  AND t."from" != 0x0000000000000000000000000000000000000000
  AND CAST(t.value AS DOUBLE) / POWER(10, a.decimals) > 0.0001
ORDER BY t.evt_block_time DESC
```

---

## Query 3: Balances Ethereum

Guárdala como: `arch_balances_ethereum`

```sql
WITH arch_tokens AS (
    SELECT * FROM (VALUES
        (0x8f0d5660929ca6ac394c5c41f59497629b1dbc23, 'WEB3', 18),
        (0xd1ce69b4bdd3dda553ea55a2a57c21c65190f3d5, 'ACAI', 18),
        (0xe15a66b7b8e385caa6f69fd0d55984b96d7263cf, 'ADDY', 18),
        (0x103bb3ebc6f61b3db2d6e01e54ef7d9899a2e16b, 'AEDY', 18),
        (0xde2925d582fc8711a0e93271c12615bdd043ed1c, 'ABDY', 18),
        (0xf436e681574220471fc72e42ae33564512dafd06, 'ARWA', 18)
    ) AS t(token_address, symbol, decimals)
),
inflows AS (
    SELECT t."to" AS wallet, t.contract_address AS token_address,
           SUM(CAST(t.value AS DOUBLE)) AS amount
    FROM erc20_ethereum.evt_Transfer t
    INNER JOIN arch_tokens a ON t.contract_address = a.token_address
    WHERE t."to" != 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
),
outflows AS (
    SELECT t."from" AS wallet, t.contract_address AS token_address,
           SUM(CAST(t.value AS DOUBLE)) AS amount
    FROM erc20_ethereum.evt_Transfer t
    INNER JOIN arch_tokens a ON t.contract_address = a.token_address
    WHERE t."from" != 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
),
net_balances AS (
    SELECT COALESCE(i.wallet, o.wallet) AS wallet,
           COALESCE(i.token_address, o.token_address) AS token_address,
           (COALESCE(i.amount, 0) - COALESCE(o.amount, 0)) AS raw_balance
    FROM inflows i
    FULL OUTER JOIN outflows o ON i.wallet = o.wallet AND i.token_address = o.token_address
)
SELECT CAST(nb.wallet AS VARCHAR) AS wallet,
       CAST(nb.token_address AS VARCHAR) AS token_address,
       a.symbol, a.decimals,
       nb.raw_balance / POWER(10, a.decimals) AS balance
FROM net_balances nb
JOIN arch_tokens a ON nb.token_address = a.token_address
WHERE nb.raw_balance / POWER(10, a.decimals) > 0.0001
ORDER BY balance DESC
```

---

## Query 4: Outflows Ethereum (desde 2026-04-01)

Guárdala como: `arch_outflows_ethereum`

```sql
WITH arch_tokens AS (
    SELECT * FROM (VALUES
        (0x8f0d5660929ca6ac394c5c41f59497629b1dbc23, 'WEB3', 18),
        (0xd1ce69b4bdd3dda553ea55a2a57c21c65190f3d5, 'ACAI', 18),
        (0xe15a66b7b8e385caa6f69fd0d55984b96d7263cf, 'ADDY', 18),
        (0x103bb3ebc6f61b3db2d6e01e54ef7d9899a2e16b, 'AEDY', 18),
        (0xde2925d582fc8711a0e93271c12615bdd043ed1c, 'ABDY', 18),
        (0xf436e681574220471fc72e42ae33564512dafd06, 'ARWA', 18)
    ) AS t(token_address, symbol, decimals)
)
SELECT CAST(t."from" AS VARCHAR) AS wallet_from,
       CAST(t."to" AS VARCHAR) AS wallet_to,
       CAST(t.contract_address AS VARCHAR) AS token_address,
       a.symbol,
       CAST(t.value AS DOUBLE) / POWER(10, a.decimals) AS amount,
       t.evt_block_time AS block_time,
       CAST(t.evt_tx_hash AS VARCHAR) AS tx_hash
FROM erc20_ethereum.evt_Transfer t
INNER JOIN arch_tokens a ON t.contract_address = a.token_address
WHERE t.evt_block_time >= TIMESTAMP '2026-04-01 00:00:00'
  AND t."from" != 0x0000000000000000000000000000000000000000
  AND CAST(t.value AS DOUBLE) / POWER(10, a.decimals) > 0.0001
ORDER BY t.evt_block_time DESC
```

---

## Después de guardar las 4 queries

Edita `config.py` y agrega los IDs al final:

```python
# Dune query IDs para queries guardadas (agrega después de guardarlas en Dune UI)
DUNE_QUERY_BALANCES_POLYGON  = XXXXXXX  # reemplaza con tu ID real
DUNE_QUERY_OUTFLOWS_POLYGON  = XXXXXXX
DUNE_QUERY_BALANCES_ETHEREUM = XXXXXXX
DUNE_QUERY_OUTFLOWS_ETHEREUM = XXXXXXX
```

Luego ejecuta el pipeline con esos IDs automáticamente.
