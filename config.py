"""
Arch Finance - Wind-Down Monitor
Configuration: all addresses, parameters, and mappings.

ARCHITECTURE NOTES:
- "Archemist" contracts are ERC-4626-like vaults that wrap Arch product tokens.
  They exchange the underlying token for USDC. Clients on Polygon hold Archemist tokens.
- On Ethereum, clients hold the direct Arch token contracts (no Archemist wrapper).
- The "Arch Ramp" (0x6eEabA794883F7...) handles fiat on/off-ramps.
- The "Trade Issuer V3" swaps tokens for later issuance.
- Outflow = any transfer of Arch tokens FROM a monitored wallet.
"""

from datetime import datetime

# =============================================================================
# PARAMETERS
# =============================================================================
CUTOFF_DATE = "2026-04-01"  # Window start for outflow analysis
DUST_THRESHOLD_USD = 1.0     # Minimum USD to consider a wallet "active"
DUST_THRESHOLD_TOKENS = 0.001  # Minimum tokens to consider non-zero

# Dune API
DUNE_API_BASE = "https://api.dune.com/api/v1"

# Existing Dune query IDs (from arch-dune-queries skill)
DUNE_QUERY_SUPPLY_ETH = 7370113  # Supply en Ethereum (WEB3, CHAIN, ACAI, ADDY, AEDY, ABDY)
DUNE_QUERY_SUPPLY_POL = 6963145  # Supply en Polygon (todos los tokens: base + portfolios)
DUNE_QUERY_POOLS  = 3591853  # Pool liquidity — se usa como fallback de precios
# DUNE_QUERY_PRICES (6963204) eliminado — reemplazado por Google Sheet

# Custom queries saved in Dune UI (April 2026)
DUNE_QUERY_BALANCES_POLYGON  = 6993335
DUNE_QUERY_OUTFLOWS_POLYGON  = 6993341
DUNE_QUERY_BALANCES_ETHEREUM = 6993356
DUNE_QUERY_OUTFLOWS_ETHEREUM = 6993368

# Google Sheets — precios diarios por token (fuente primaria de precios, gratis)
GSHEET_PRICES_CSV = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vTRo8ZMelWUvWRGixaxnpeKsgRU7WPck5Tiv3Dn7s16NYQSVAkDJLr54IHjuM7Jqq1Z7a9ho3k59dN6"
    "/pub?gid=1638022190&single=true&output=csv"
)
# Columnas del sheet → símbolos base Arch (incluye vault tokens directamente)
GSHEET_PRICE_COLUMNS = ["WEB3", "CHAIN", "ACAI", "ADDY", "AEDY", "ABDY", "ABAL", "AMOD", "AAGG"]

# =============================================================================
# PORTFOLIO VAULT POSITIONS (hardcoded desde getPositions() en Polygon)
# Unidades de cada token subyacente por 1 token de portfolio.
# NAV = sum(unidades_i * precio_i)
# =============================================================================
VAULT_POSITIONS = {
    "AAGG": {
        "CHAIN": 0.31000,
        "AEDY":  0.43980,
        "ABDY":  0.89180,
        "WEB3":  0.23347,
    },
    "AMOD": {
        "CHAIN": 0.34740,
        "WEB3":  0.97760,
        "ADDY":  0.90430,
        "AEDY":  0.37299,
        "ABDY":  0.21870,
    },
    "ABAL": {
        "WEB3":  0.60100,
        "CHAIN": 0.20742,
        "ADDY":  0.29318,
        "AEDY":  0.21994,
        "ABDY":  0.13533,
    },
    "AP60": {  # legacy de AMOD — mismas posiciones
        "CHAIN": 0.34740,
        "WEB3":  0.97760,
        "ADDY":  0.90430,
        "AEDY":  0.37299,
        "ABDY":  0.21870,
    },
}

# =============================================================================
# ARCH TOKENS - POLYGON (Archemist contracts = what clients hold)
# =============================================================================
ARCHEMIST_TOKENS_POLYGON = {
    "0xc68140cdf17566f8ad43db8487d6600196d79176": {"symbol": "WEB3", "decimals": 18, "exchange_token": "USDC", "fee": 0.005},
    "0xc770c918332522ac66306a83989ba1b5b807c1ae": {"symbol": "CHAIN", "decimals": 18, "exchange_token": "USDC", "fee": 0.005},
    "0x2c0c8a17a58d37f0ca75cf9482307a8c6043d252": {"symbol": "ACAI", "decimals": 18, "exchange_token": "USDC", "fee": 0.005},
    "0xcfa916cceb4a32d727210b21b7a23fbccc5c4e7d": {"symbol": "ABDY", "decimals": 18, "exchange_token": "USDC", "fee": 0.005},
    "0x774aac3a6f0da70d57621de098cf4d2ef77bf1a5": {"symbol": "ADDY", "decimals": 18, "exchange_token": "USDC", "fee": 0.001},
    "0x578934df6e0f1d525ee5ce3397a7c4c554945efa": {"symbol": "AEDY", "decimals": 18, "exchange_token": "USDC", "fee": 0.005},
}

# Underlying product tokens on Polygon (some clients might hold these directly)
PRODUCT_TOKENS_POLYGON = {
    "0xc4ea087fc2cb3a1d9ff86c676f03abe4f3ee906f": {"symbol": "WEB3_PROD", "decimals": 18},
    "0x70a13201df2364b634cb5aac8d735db3a654b30c": {"symbol": "CHAIN_PROD", "decimals": 18},
    "0x9f5c845a178dfcb9abe1e9d3649269826ce43901": {"symbol": "ACAI_PROD", "decimals": 18},
    "0xab1b1680f6037006e337764547fb82d17606c187": {"symbol": "ADDY_PROD", "decimals": 18},
    "0x027af1e12a5869ed329be4c05617ad528e997d5a": {"symbol": "AEDY_PROD", "decimals": 18},
    "0xef7b6cd33afafc36379289b7accae95116e27c88": {"symbol": "ABDY_PROD", "decimals": 18},
}

# Portfolio tokens on Polygon
PORTFOLIO_TOKENS_POLYGON = {
    "0xafb6e8331355fae99c8e8953bb4c6dc5d11e9f3c": {"symbol": "AAGG", "decimals": 18},
    "0xa5a979aa7f55798e99f91abe815c114a09164beb": {"symbol": "AMOD", "decimals": 18},
    "0xf401e2c1ce8f252947b60bfb92578f84217a1545": {"symbol": "ABAL", "decimals": 18},
    "0x6ca9c8914a14d63a6700556127d09e7721ff7d3b": {"symbol": "AP60", "decimals": 18},
}

# SET tokens on Polygon
SET_TOKENS_POLYGON = {
    "0x9a41e03fef7f16f552c6fba37ffa7590fb1ec0c4": {"symbol": "CHAIN_SET", "decimals": 18},
    "0xbcd2c5c78000504efbc1ce6489dfcac71835406a": {"symbol": "WEB3_SET", "decimals": 18},
}

# Legacy tokens
LEGACY_TOKENS_POLYGON = {
    "0xde2925d582fc8711a0e93271c12615bdd043ed1c": {"symbol": "ABDY_V1", "decimals": 18},
}

# ALL Polygon tokens combined
ALL_TOKENS_POLYGON = {
    **ARCHEMIST_TOKENS_POLYGON,
    **PRODUCT_TOKENS_POLYGON,
    **PORTFOLIO_TOKENS_POLYGON,
    **SET_TOKENS_POLYGON,
    **LEGACY_TOKENS_POLYGON,
}

# =============================================================================
# ARCH TOKENS - ETHEREUM (direct tokens, held by preferente clients)
# =============================================================================
TOKENS_ETHEREUM = {
    "0x8f0d5660929ca6ac394c5c41f59497629b1dbc23": {"symbol": "WEB3",      "decimals": 18},
    "0x89c53b02558e4d1c24b9bf3bed1279871187ef0b": {"symbol": "CHAIN",     "decimals": 18},
    "0xd1ce69b4bdd3dda553ea55a2a57c21c65190f3d5": {"symbol": "ACAI",      "decimals": 18},
    "0xe15a66b7b8e385caa6f69fd0d55984b96d7263cf": {"symbol": "ADDY",      "decimals": 18},
    "0x103bb3ebc6f61b3db2d6e01e54ef7d9899a2e16b": {"symbol": "AEDY",      "decimals": 18},
    "0xde2925d582fc8711a0e93271c12615bdd043ed1c": {"symbol": "ABDY",      "decimals": 18},
    # SET versions
    "0x0d20e86abab680c038ac8bbdc1446585e67f8951": {"symbol": "CHAIN_SET", "decimals": 18},
    "0xe8e8486228753e01dbc222da262aa706bd67e601": {"symbol": "WEB3_SET",  "decimals": 18},
}

# =============================================================================
# ARCH OPERATIONAL CONTRACTS (outflow destinations)
# =============================================================================
ARCH_CONTRACTS_POLYGON = {
    # ── Operational & Peripheral (from Official Blockchain Addresses PDF) ──
    "0x6eeaba794883f75a1e6e9a38426207e853a6df58": "Arch Ramp (On & OffRamps)",
    "0xf67df2fd4a56046eacf03e3762b2495cfdedf271": "Gasworks (User Operations)",
    "0xbc13b615c6630326a15e312c345619da756226a1": "Migration Escrow",
    "0xdcb99117ba207b996ee3c49ee6f8c0f1d371867a": "Trade Issuer V3",
    "0xfde21d887b245849e2509163582ce0bbc90fcc4c": "Arch Nexus",
    "0x66ee243e25d67dcec02874102f68809a597060bd": "Faucet",
    "0x3ddd928a5d1be641c0bf2727a078f1342a1a6c0e": "CEX Ramp",
    "0xd461ecac15cc2891a588ce283933065d1125db6c": "Koywe Ramp (OnRamps)",
    "0xa8b21f3cbc89e6d88f31b9486aa5a5c37560e471": "Koywe OffRamp",
    "0x9ea1f32a606a2956345444aa7c0dcfe6ccab30f4": "Gasless",
    "0x217216913438fa9e305187727963dbf595d4d796": "Referrals",
    "0x7f7214c19a2ad6c5a7d07d2e187de1a008a7bea9": "Fiat Ramp (DCA)",
    "0xf33f0262dd37c9ae09393d09764aa363dcdc9627": "Development Ops",
    "0x131067246bbd3c94c82e0b74c71d430e81da950b": "Liquidity Manager",
    "0x5953e8e6070287c63ee95480a4768faa5dd3f405": "Archemist Operator",
    "0xe1e9568b9f735cafb282bb164687d4c37587bf90": "Archemist God (Factory)",
    "0xe560efd37a77486aa0ecaed4203365bde5363dbb": "Trade Issuer Operator",
    "0xb2709612c105b86c44ba0150456e47ca248d7685": "Backoffice Login",
    "0xb3f2cc719dcadca9133074aa37964cb972fb3d82": "Structured Funds Factory",
    "0xf01c18deef438f3a5e4bb27404b4b44911625300": "Arch Leverage ETH",
    "0x0a0044e0521ccd7cd61fe4c943e2e95b149659e9": "ALPS (Liquidity Position Strategy)",
    # ── Chamber Ecosystem ──
    "0x0c9aa1e4b4e39da01b7459607995368e4c38cfef": "Chamber God (Factory)",
    "0x60f56236cd3c1ac146bd94f2006a1335baa4c449": "Issuer Wizard",
    "0xdd5211d669f5b1f19991819bbd8b220dbbf8062e": "Streaming Fee Wizard",
    "0x13541ea37cfb0ce3bff8f28d468d93b348bcddea": "Rebalance Wizard",
    # ── All token contract addresses (they hold reserves, not client wallets) ──
    **{addr: f"Archemist {info['symbol']}" for addr, info in ARCHEMIST_TOKENS_POLYGON.items()},
    **{addr: f"Token {info['symbol']}" for addr, info in PRODUCT_TOKENS_POLYGON.items()},
    **{addr: f"Portfolio {info['symbol']}" for addr, info in PORTFOLIO_TOKENS_POLYGON.items()},
    **{addr: f"SET {info['symbol']}" for addr, info in SET_TOKENS_POLYGON.items()},
    **{addr: f"Legacy {info['symbol']}" for addr, info in LEGACY_TOKENS_POLYGON.items()},
}

ARCH_CONTRACTS_ETHEREUM = {
    "0x3ddd928a5d1be641c0bf2727a078f1342a1a6c0e": "CEX Ramp (ETH)",
    "0x131067246bbd3c94c82e0b74c71d430e81da950b": "Liquidity Manager (ETH)",
    "0x92b6a2aee6c748ad196fbfd449f87c9b2aa2e519": "Trade Issuer V3 (ETH)",
}

BURN_ADDRESS = "0x0000000000000000000000000000000000000000"

# =============================================================================
# USDC ADDRESSES
# =============================================================================
USDC_POLYGON_NATIVE = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
USDC_POLYGON_BRIDGED = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
USDC_ETHEREUM = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

# =============================================================================
# HELPER: Canonical symbol mapping (normalize to base symbol for pricing)
# =============================================================================
SYMBOL_TO_BASE = {
    "WEB3": "WEB3", "WEB3_PROD": "WEB3", "WEB3_SET": "WEB3",
    "CHAIN": "CHAIN", "CHAIN_PROD": "CHAIN", "CHAIN_SET": "CHAIN",
    "ACAI": "ACAI", "ACAI_PROD": "ACAI",
    "ABDY": "ABDY", "ABDY_PROD": "ABDY", "ABDY_V1": "ABDY",
    "ADDY": "ADDY", "ADDY_PROD": "ADDY",
    "AEDY": "AEDY", "AEDY_PROD": "AEDY",
    "AAGG": "AAGG", "AMOD": "AMOD", "ABAL": "ABAL", "AP60": "AP60",
}

# Archemist address → symbol (for quick lookup)
ARCHEMIST_ADDR_TO_SYMBOL = {addr: info["symbol"] for addr, info in ARCHEMIST_TOKENS_POLYGON.items()}

def classify_outflow_destination(dest_address: str) -> str:
    """Classify where tokens went based on destination address."""
    dest = dest_address.lower()
    if dest == BURN_ADDRESS:
        return "Burn (Redeem)"
    if dest in ARCH_CONTRACTS_POLYGON:
        return ARCH_CONTRACTS_POLYGON[dest]
    if dest in ARCH_CONTRACTS_ETHEREUM:
        return ARCH_CONTRACTS_ETHEREUM[dest]
    if dest in ARCHEMIST_TOKENS_POLYGON:
        return f"Archemist Redeem ({ARCHEMIST_TOKENS_POLYGON[dest]['symbol']})"
    return "Transfer to External Wallet"
