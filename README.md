# Arch Finance — Wind-Down Monitor

Sistema de monitoreo para el proceso de cierre de Arch Finance. Extrae datos on-chain via Dune Analytics, los consolida en Python, y los visualiza en un dashboard Streamlit.

---

## 1. Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                        FUENTES DE DATOS                         │
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐    │
│  │ Dune: Prices │   │ Dune: Supply │   │ Dune: Pools      │    │
│  │ Query 6963204│   │ Query 6963145│   │ Query 3591853    │    │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────────┘    │
│         │                  │                   │                │
│  ┌──────┴──────────────────┴───────────────────┴─────────┐     │
│  │ Dune Custom Queries (SQL en dune_queries.py)          │     │
│  │  Q1: Balances Polygon (all Arch token holders)        │     │
│  │  Q2: Balances Ethereum                                │     │
│  │  Q3: Outflows Polygon (transfers since 2026-04-01)    │     │
│  │  Q4: Outflows Ethereum                                │     │
│  │  Q5: USDC inflows (validación cruzada)                │     │
│  └──────────────────────┬────────────────────────────────┘     │
│                         │                                       │
│                   CSV / API ↓                                   │
│                                                                 │
│  ┌──────────────────────┴────────────────────────────────┐     │
│  │              PIPELINE PYTHON (pipeline.py)             │     │
│  │                                                        │     │
│  │  1. Carga wallets (clientes.xlsx)                      │     │
│  │  2. Descarga o lee CSVs de Dune                        │     │
│  │  3. Filtra a wallets monitoreadas                      │     │
│  │  4. Clasifica outflows por destino                     │     │
│  │  5. Computa métricas por wallet y globales             │     │
│  │  6. Genera datasets procesados en data/processed/      │     │
│  └──────────────────────┬────────────────────────────────┘     │
│                         │                                       │
│                    Archivos ↓                                   │
│                                                                 │
│  ┌──────────────────────┴────────────────────────────────┐     │
│  │           DASHBOARD STREAMLIT (app.py)                 │     │
│  │                                                        │     │
│  │  - KPIs: remanente, retirado, %, por segmento          │     │
│  │  - Charts: timeline, distribución, pie de estados      │     │
│  │  - Tablas: resumen wallet, balances, outflows, tops    │     │
│  │  - Filtros: segmento, red, estado, saldo mínimo        │     │
│  └────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

### Qué hace cada capa:
- **Dune**: Extrae datos on-chain crudos (balances de tokens, transferencias, precios, pools)
- **Python (pipeline.py)**: Limpia, filtra a clientes Arch, clasifica outflows, computa métricas
- **Streamlit (app.py)**: Visualiza todo en un dashboard interactivo con filtros

---

## 2. Definición Técnica de Outflows

### Qué se considera outflow:
Un outflow es **cualquier transferencia ERC-20 de tokens Arch DESDE una wallet monitoreada** ocurrida después del `CUTOFF_DATE` (2026-04-01).

### Clasificación por destino:
| Destino | Tipo | Interpretación |
|---------|------|----------------|
| Arch Ramp (0x6eEa...) | Ramp Redemption | Cliente usa el off-ramp oficial |
| Archemist contract | Archemist Redeem | Redención directa del vault |
| Trade Issuer V3 | Trade Issuer | Swap via chamber ecosystem |
| Gasworks | Gasworks | Operación de usuario |
| 0x0000...0000 | Burn | Token quemado (redención) |
| Otra wallet monitoreada | Transfer Interno | NO cuenta como outflow real |
| Cualquier otra dirección | Transfer External | Salida a wallet externa o DEX |

### Exclusiones:
- Transferencias internas entre wallets monitoreadas (flag `is_internal_transfer`)
- Montos menores al `DUST_THRESHOLD_TOKENS` (0.001 tokens)

### Riesgos:
- **Falso positivo**: Transfer a otra wallet propia del cliente (no es redención real). Mitigación: revisar manualmente transfers a wallets externas.
- **Falso negativo**: Redención via contrato proxy no mapeado. Mitigación: capturamos TODAS las transferencias salientes, no solo a contratos conocidos.

---

## 3. Estructura de Datos

### Archivos generados en `data/processed/`:

| Archivo | Descripción |
|---------|-------------|
| `wallets_normalized.csv` | Lista normalizada de wallets (1 row por wallet-red) |
| `balances.csv` | Balance actual por wallet × token |
| `outflows.csv` | Detalle de cada transferencia saliente |
| `wallet_summary.csv` | Resumen consolidado por wallet |
| `global_metrics.json` | KPIs globales |
| `pools.csv` | Datos de pools de liquidez |
| `supply.csv` | Supply de cada token |

### Columnas clave de `wallet_summary.csv`:
```
wallet_address, customer_name, email, segment, network,
status, total_balance_usd, total_outflow_usd, pct_withdrawn,
num_outflow_events, last_outflow_date, tokens_held, outflow_tokens
```

### Estados posibles de un wallet:
- **Sin movimiento**: tiene saldo, no ha retirado
- **Retiro parcial**: tiene saldo Y ha retirado
- **Retirado completamente**: sin saldo, con retiros detectados
- **Sin saldo (sin retiro detectado)**: sin saldo y sin actividad post-cutoff

---

## 4. Setup y Uso

### Instalación:
```bash
pip install -r requirements.txt
```

### Opción A: Con Dune API key (automático)
```bash
python pipeline.py --api-key tu_dune_api_key --wallets wallets/clientes.xlsx
```

### Opción B: Con CSVs manuales (sin API key)
1. Ejecuta cada query en dune.com (ver `dune_queries.py` o el dashboard)
2. Descarga los resultados como CSV
3. Nómbralos: `balances_polygon.csv`, `balances_ethereum.csv`, `outflows_polygon.csv`, `outflows_ethereum.csv`, `prices.csv`, `supply.csv`, `pools.csv`
4. Colócalos en `data/raw/`
5. Ejecuta:
```bash
python pipeline.py --csv-dir ./data/raw/ --wallets wallets/clientes.xlsx
```

### Opción C: Mixta (API + CSV fallback)
```bash
python pipeline.py --api-key tu_key --csv-dir ./data/raw/ --wallets wallets/clientes.xlsx
```

### Levantar el dashboard:
```bash
streamlit run app.py
```

---

## 5. Queries de Dune

Las queries están en `dune_queries.py`. Para verlas formateadas:
```bash
python dune_queries.py
```

| Query | Descripción | Para usar en Dune UI |
|-------|-------------|---------------------|
| Q1 | Balances Polygon | Copiar SQL de `query_balances_polygon()` |
| Q2 | Balances Ethereum | Copiar SQL de `query_balances_ethereum()` |
| Q3 | Outflows Polygon | Copiar SQL de `query_outflows_polygon()` |
| Q4 | Outflows Ethereum | Copiar SQL de `query_outflows_ethereum()` |
| Q5 | USDC Inflows (validación) | Copiar SQL de `query_usdc_inflows_polygon()` |
| 6963204 | Precios actuales | Ya existe en Dune |
| 6963145 | Supply por token | Ya existe en Dune |
| 3591853 | Pools de liquidez | Ya existe en Dune |

---

## 6. Cómo Re-ejecutar

Para obtener una foto actualizada:
1. Re-ejecuta las queries en Dune (o usa la API)
2. Descarga los nuevos CSVs
3. Corre `python pipeline.py --csv-dir ./data/raw/`
4. Refresca el dashboard (F5 en el browser)

---

## 7. Validaciones Recomendadas

### Validaciones automáticas:
- El pipeline logea cuántas wallets monitoreadas aparecen en los datos de Dune
- Se excluyen internal transfers para no doble-contar
- Se aplica dust threshold para ignorar residuos

### Validaciones manuales:
- [ ] Verificar en Polygonscan la wallet ejemplo (0x8282Cb...) para confirmar que los outflows detectados coinciden con transacciones reales
- [ ] Comparar `total_remaining + total_withdrawn` contra el AUM conocido
- [ ] Revisar wallets con "Transfer External" — pueden ser false positives
- [ ] Verificar que los precios de Dune (query 6963204) estén actualizados
- [ ] Para ETH preferentes: confirmar que las direcciones de tokens ETH son correctas (algunas se reconstruyeron del PDF)

### Edge cases:
- Wallets que reciben tokens DESPUÉS del cutoff (inflows post-cierre)
- Tokens migrados entre versiones (ABDY V1 → ABDY V2)
- Wallets preferentes con presencia en ambas redes
- Dust balances que pueden generar ruido
- Tokens de portfolio (AAGG, AMOD, ABAL, AP60) que pueden tener flujos diferentes

---

## 8. Datos del Negocio

### Universo de wallets:
- **128 preferentes** (12 solo ETH, 115 solo Polygon, 1 ambas)
- **2,813 retail** (todos Polygon)
- **Total: ~2,942 wallet-network entries**

### Tokens monitoreados:
- **Polygon Archemist** (6): WEB3, CHAIN, ACAI, ABDY, ADDY, AEDY
- **Polygon Product** (6): versiones subyacentes
- **Polygon Portfolio** (4): AAGG, AMOD, ABAL, AP60
- **Polygon SET** (2): CHAIN_SET, WEB3_SET
- **Polygon Legacy** (1): ABDY_V1
- **Ethereum** (9): WEB3, CHAIN, ACAI, ADDY, AEDY, ABDY, ARWA, CHAIN_SET, WEB3_SET

### Contratos operacionales clave:
- **Arch Ramp**: 0x6eEabA794883F75a1e6E9a38426207e853a6Df58 (On & OffRamps)
- **Trade Issuer V3**: 0xdCB99117Ba207b996EE3c49eE6F8c0f1d371867A
- **Gasworks**: 0xf67df2fd4a56046eacf03e3762b2495cfdedf271
