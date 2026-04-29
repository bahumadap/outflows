"""
Arch Finance - Wind-Down Monitor Dashboard
Design: borders-only depth, 8px grid, semantic color only
"""

import json
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Arch Finance · Cierre",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── DESIGN SYSTEM ────────────────────────────────────────────────────────────
# Direction:  Technical data-heavy admin tool
# Depth:      Borders-only (clean, no shadows)
# Spacing:    8px base unit, consistent multiples
# Color:      Monochrome structure, semantic only
# Type:       Tight tracking on labels, clear weight hierarchy
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
/* Reset & base */
* { box-sizing: border-box; }
[data-testid="stAppViewContainer"] { background: #0D1117; }
[data-testid="stSidebar"] { background: #0D1117; border-right: 1px solid #1C2333; }
[data-testid="stSidebar"] * { font-size: 13px; }
[data-testid="block-container"] { padding: 24px 32px 48px 32px; }
div[data-testid="stVerticalBlock"] > div { gap: 0px; }

/* Typography scale (8px base) */
.t-label  { font-size: 11px; font-weight: 600; letter-spacing: 0.09em; text-transform: uppercase; color: #4B5675; line-height: 1.4; }
.t-value  { font-size: 28px; font-weight: 700; letter-spacing: -0.02em; color: #CDD5E0; line-height: 1.1; }
.t-value-lg { font-size: 36px; font-weight: 700; letter-spacing: -0.03em; color: #CDD5E0; line-height: 1.0; }
.t-sub    { font-size: 12px; color: #4B5675; margin-top: 4px; line-height: 1.4; }
.t-accent { font-weight: 700; }

/* Semantic colors */
.c-green  { color: #3DD68C; }
.c-red    { color: #F87171; }
.c-blue   { color: #60A5FA; }
.c-orange { color: #FB923C; }
.c-dim    { color: #4B5675; }

/* Card — borders only, no shadow */
.card {
    border: 1px solid #1C2333;
    border-radius: 8px;
    padding: 20px 20px 16px 20px;
    background: transparent;
    height: 100%;
}
.card-inner {
    border: 1px solid #1C2333;
    border-radius: 6px;
    padding: 16px;
    background: transparent;
    margin-bottom: 8px;
}

/* Section divider */
.section-title {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #2D3650;
    margin: 32px 0 16px 0;
    padding-bottom: 8px;
    border-bottom: 1px solid #1C2333;
}

/* Progress track */
.track { background: #1C2333; border-radius: 4px; height: 6px; margin: 8px 0 6px 0; overflow: hidden; }
.track-fill-green  { background: #3DD68C; height: 100%; border-radius: 4px; transition: width 0.3s ease; }
.track-fill-red    { background: #F87171; height: 100%; border-radius: 4px; }
.track-fill-blue   { background: #60A5FA; height: 100%; border-radius: 4px; }
.track-fill-orange { background: #FB923C; height: 100%; border-radius: 4px; }

/* Stat row */
.stat-row { display:flex; justify-content:space-between; align-items:center; padding: 6px 0; border-bottom: 1px solid #111827; }
.stat-row:last-child { border-bottom: none; }
.stat-k { font-size: 12px; color: #4B5675; }
.stat-v { font-size: 12px; font-weight: 600; color: #9CA3AF; }

/* Status dots */
.dot-green  { color: #3DD68C; }
.dot-red    { color: #F87171; }
.dot-yellow { color: #FBBF24; }
.dot-dim    { color: #2D3650; }

/* Sidebar items */
.sidebar-kpi { padding: 12px 0; border-bottom: 1px solid #1C2333; }
.sidebar-kpi:last-child { border-bottom: none; }
</style>
""", unsafe_allow_html=True)

# Colors for Plotly (consistent semantic palette)
C_GREEN  = "#3DD68C"
C_RED    = "#F87171"
C_BLUE   = "#60A5FA"
C_ORANGE = "#FB923C"
C_DIM    = "#2D3650"
C_GRID   = "#1C2333"
C_BG     = "#0D1117"
C_TEXT   = "#9CA3AF"

PLOTLY_BASE = dict(
    plot_bgcolor=C_BG, paper_bgcolor=C_BG,
    font=dict(color=C_TEXT, family="Inter, system-ui, sans-serif", size=12),
    margin=dict(t=24, b=8, l=8, r=8),
)

PROCESSED_DIR = Path("data/processed")


# ─── DATA ─────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_data():
    data = {}
    mp = PROCESSED_DIR / "global_metrics.json"
    data["metrics"] = json.load(open(mp)) if mp.exists() else {}
    pp = PROCESSED_DIR / "prices.json"
    data["prices"] = json.load(open(pp)) if pp.exists() else {}
    for name in ["balances", "outflows", "wallet_summary", "pools", "supply", "unknown_wallets", "reconciliation", "contract_balances"]:
        path = PROCESSED_DIR / f"{name}.csv"
        if path.exists():
            df = pd.read_csv(path)
            for col in ["block_time", "last_outflow_date"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
            data[name] = df
        else:
            data[name] = pd.DataFrame()
    return data

def check_data():
    return (PROCESSED_DIR / "global_metrics.json").exists()


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def fmt(v, prefix="$"):
    if v >= 1_000_000: return f"{prefix}{v/1_000_000:.2f}M"
    if v >= 1_000:     return f"{prefix}{v/1_000:.1f}K"
    return f"{prefix}{v:,.0f}"

def pct(num, den): return (num / max(den, 1)) * 100

def track(fill_pct: float, cls: str) -> str:
    w = min(max(fill_pct, 0), 100)
    return f'<div class="track"><div class="{cls}" style="width:{w:.1f}%"></div></div>'

def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    if df.empty: return df
    r = df.copy()
    if "segment" in r.columns and filters.get("segment"):
        r = r[r["segment"].isin(filters["segment"])]
    if "network" in r.columns and filters.get("network"):
        r = r[r["network"].isin(filters["network"])]
    if "status" in r.columns and filters.get("status"):
        r = r[r["status"].isin(filters["status"])]
    if "total_balance_usd" in r.columns and filters.get("min_bal", 0) > 0:
        r = r[r["total_balance_usd"] >= filters["min_bal"]]
    return r


# ─── SIDEBAR ──────────────────────────────────────────────────────────────────

def render_sidebar(ws: pd.DataFrame) -> dict:
    with st.sidebar:
        st.markdown("**⬡ Arch Finance**  \nMonitor de Cierre")
        st.divider()

        st.markdown("**Filtros**")
        seg = st.multiselect("Segmento", ["preferente", "retail", "sin_registro"], default=["preferente", "retail", "sin_registro"], label_visibility="collapsed")
        st.caption("Segmento")
        net = st.multiselect("Red", ["polygon", "ethereum"], default=["polygon", "ethereum"], label_visibility="collapsed")
        st.caption("Red")

        status_opts = sorted(ws["status"].dropna().unique().tolist()) if not ws.empty else []
        status = st.multiselect("Estado", status_opts, default=status_opts, label_visibility="collapsed")
        st.caption("Estado")

        min_bal = st.number_input("Saldo mín. USD", min_value=0.0, value=0.0, step=100.0)

        st.divider()

        # Sidebar KPIs — quick reference
        if not ws.empty:
            ws_f = apply_filters(ws, {"segment": seg, "network": net, "status": status, "min_bal": min_bal})
            rem = ws_f["total_balance_usd"].sum()
            out = ws_f["total_outflow_usd"].sum()
            aum = rem + out
            st.markdown(
                f"<div class='sidebar-kpi'><div class='t-label'>Remanente</div><div style='font-size:18px;font-weight:700;color:#3DD68C'>{fmt(rem)}</div><div class='t-sub'>{pct(rem,aum):.1f}% del AUM</div></div>"
                f"<div class='sidebar-kpi'><div class='t-label'>Retirado</div><div style='font-size:18px;font-weight:700;color:#F87171'>{fmt(out)}</div><div class='t-sub'>{pct(out,aum):.1f}% del AUM</div></div>"
                f"<div class='sidebar-kpi'><div class='t-label'>AUM total</div><div style='font-size:18px;font-weight:700;color:#9CA3AF'>{fmt(aum)}</div></div>",
                unsafe_allow_html=True)

        st.divider()
        data = load_data()
        ts = data.get("metrics", {}).get("timestamp", "—")
        st.caption(f"Datos: {ts[:19] if ts and ts != '—' else '—'}")
        st.caption("`python pipeline.py --csv-dir ./data/raw/` → F5")

    return {"segment": seg, "network": net, "status": status, "min_bal": min_bal}


# ─── OVERVIEW ─────────────────────────────────────────────────────────────────

def render_overview(ws: pd.DataFrame):
    rem   = ws["total_balance_usd"].sum()
    out   = ws["total_outflow_usd"].sum()
    aum   = rem + out
    p_out = pct(out, aum)
    p_rem = pct(rem, aum)

    n_total    = len(ws)
    n_done     = (ws["status"] == "Retirado completamente").sum()
    n_partial  = (ws["status"] == "Retiro parcial").sum()
    n_withdrew = n_done + n_partial   # retiraron algo (total o parcial)
    n_pending  = (ws["total_balance_usd"] > 1).sum()  # aún tienen saldo > $1

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown(f"""
        <div class="card">
            <div class="t-label">Retirado</div>
            <div class="t-value-lg c-red">{fmt(out)}</div>
            {track(p_out, "track-fill-red")}
            <div class="t-sub"><span class="t-accent c-red">{p_out:.1f}%</span> del AUM estimado ({fmt(aum)})</div>
        </div>""", unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div class="card">
            <div class="t-label">Remanente en plataforma</div>
            <div class="t-value-lg c-green">{fmt(rem)}</div>
            {track(p_rem, "track-fill-green")}
            <div class="t-sub"><span class="t-accent c-green">{p_rem:.1f}%</span> aún sin retirar</div>
        </div>""", unsafe_allow_html=True)

    with c3:
        p_withdrew = pct(n_withdrew, n_total)
        p_pending  = pct(n_pending, n_total)
        st.markdown(f"""
        <div class="card">
            <div class="t-label">Wallets</div>
            <div style="display:flex;gap:24px;margin:8px 0 4px 0;align-items:flex-end">
                <div>
                    <div style="font-size:11px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#F87171;margin-bottom:2px">Retiraron</div>
                    <div style="font-size:28px;font-weight:700;color:#F87171;line-height:1">{n_withdrew}</div>
                    <div style="font-size:11px;color:#4B5675;margin-top:2px">{p_withdrew:.0f}% del total · {n_done} completo · {n_partial} parcial</div>
                </div>
                <div style="width:1px;background:#1C2333;align-self:stretch;margin-bottom:4px"></div>
                <div>
                    <div style="font-size:11px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#3DD68C;margin-bottom:2px">Pendientes</div>
                    <div style="font-size:28px;font-weight:700;color:#3DD68C;line-height:1">{n_pending}</div>
                    <div style="font-size:11px;color:#4B5675;margin-top:2px">{p_pending:.0f}% del total · con saldo &gt; $1</div>
                </div>
            </div>
            <div style="font-size:11px;color:#4B5675;margin-top:6px">{n_total:,} wallets totales</div>
        </div>""", unsafe_allow_html=True)

    # ── Price strip ───────────────────────────────────────────────────────────
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
    data = load_data()

    # Precios desde prices.json si existe (generado por el pipeline, fuente primaria)
    prices_display = {k: v for k, v in data.get("prices", {}).items() if v > 0}

    # Fallback si prices.json aún no existe: leer pools + balances (comportamiento anterior)
    if not prices_display:
        pools_fb = data.get("pools", pd.DataFrame())
        if not pools_fb.empty and "token" in pools_fb.columns and "price" in pools_fb.columns:
            for _, r in pools_fb[pools_fb["token"] != "USDC"].iterrows():
                try:
                    prices_display[r["token"]] = float(r["price"])
                except (ValueError, TypeError):
                    pass
        bal_fb = data.get("balances", pd.DataFrame())
        if not bal_fb.empty and "base_symbol" in bal_fb.columns and "price_usd" in bal_fb.columns:
            for sym in ["AAGG", "AMOD", "ABAL", "AP60"]:
                rows = bal_fb[bal_fb["base_symbol"] == sym]
                if not rows.empty:
                    p = rows["price_usd"].iloc[0]
                    if p > 0:
                        prices_display[sym] = float(p)

    TOKEN_COLORS = {
        "WEB3": "#10B981", "CHAIN": "#3B82F6", "ABDY": "#8B5CF6",
        "AEDY": "#6366F1", "ADDY": "#F59E0B", "ACAI": "#EC4899",
        "AAGG": "#64748B", "AMOD": "#64748B", "ABAL": "#64748B",
    }
    ORDER = ["WEB3", "CHAIN", "ABDY", "AEDY", "ADDY", "ACAI", "AAGG", "AMOD", "ABAL"]
    tokens_to_show = [t for t in ORDER if t in prices_display]

    if tokens_to_show:
        cols = st.columns(len(tokens_to_show))
        for i, sym in enumerate(tokens_to_show):
            price = prices_display[sym]
            color = TOKEN_COLORS.get(sym, "#6B7A99")
            is_vault = sym in ("AAGG", "AMOD", "ABAL")
            label_extra = "<div style='font-size:9px;color:#2D3650;margin-top:1px'>NAV</div>" if is_vault else ""
            with cols[i]:
                st.markdown(
                    f"<div style='border:1px solid #1C2333;border-radius:6px;padding:10px 12px;text-align:center'>"
                    f"<div style='font-size:11px;font-weight:700;color:{color};letter-spacing:0.05em'>{sym}</div>"
                    f"{label_extra}"
                    f"<div style='font-size:15px;font-weight:700;color:#CDD5E0;margin-top:4px;letter-spacing:-0.01em'>${price:,.4f}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )


# ─── SEGMENTS ─────────────────────────────────────────────────────────────────

def render_segments(ws: pd.DataFrame):
    st.markdown('<div class="section-title">Desglose por Segmento</div>', unsafe_allow_html=True)

    pref    = ws[ws["segment"] == "preferente"]
    retail  = ws[ws["segment"] == "retail"]
    sinreg  = ws[ws["segment"] == "sin_registro"]

    def seg_card(df, label, track_cls, c_accent):
        if df.empty:
            return
        rem = df["total_balance_usd"].sum()
        out = df["total_outflow_usd"].sum()
        aum = rem + out
        p_o = pct(out, aum)
        p_r = pct(rem, aum)
        n   = len(df)
        n_done    = int((df["status"] == "Retirado completamente").sum())
        n_partial = int((df["status"] == "Retiro parcial").sum())
        n_sinmov  = int((df["status"] == "Sin movimiento").sum())

        TD  = "style='padding:5px 0;border-bottom:1px solid #111827;'"
        TDK = "style='font-size:12px;color:#4B5675'"
        TDV = "style='font-size:12px;font-weight:600;color:#9CA3AF;text-align:right'"

        html = (
            f"<div class='card'>"
            f"<div class='t-label'>{label}</div>"
            f"<div style='margin-top:12px;margin-bottom:4px;display:flex;justify-content:space-between;align-items:baseline'>"
            f"<span style='font-size:22px;font-weight:700;letter-spacing:-0.02em;color:{c_accent}'>{fmt(out)}</span>"
            f"<span style='font-size:12px;font-weight:700;color:{c_accent}'>{p_o:.1f}% retirado</span>"
            f"</div>"
            f"{track(p_o, track_cls)}"
            f"<div class='t-sub' style='margin-bottom:16px'>Remanente: <span style='color:#9CA3AF;font-weight:600'>{fmt(rem)}</span> &middot; {p_r:.1f}%</div>"
            f"<table style='width:100%;border-collapse:collapse'>"
            f"<tr {TD}><td {TDK}>AUM estimado</td><td {TDV}>{fmt(aum)}</td></tr>"
            f"<tr {TD}><td {TDK}>Wallets totales</td><td {TDV}>{n:,}</td></tr>"
            f"<tr {TD}><td {TDK}>Retirados completo</td><td style='font-size:12px;font-weight:600;color:#F87171;text-align:right'>{n_done}</td></tr>"
            f"<tr {TD}><td {TDK}>Retiro parcial</td><td style='font-size:12px;font-weight:600;color:#FBBF24;text-align:right'>{n_partial}</td></tr>"
            f"<tr style='padding:5px 0'><td {TDK}>Sin movimiento</td><td style='font-size:12px;font-weight:600;color:#4B5675;text-align:right'>{n_sinmov}</td></tr>"
            f"</table>"
            f"</div>"
        )
        st.markdown(html, unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1: seg_card(pref,   "🏢  PREFERENTE",   "track-fill-blue",   C_BLUE)
    with c2: seg_card(retail, "👤  RETAIL",        "track-fill-orange", C_ORANGE)
    with c3: seg_card(sinreg, "🔍  SIN REGISTRO",  "track-fill-red",    "#94A3B8")


# ─── CHARTS ───────────────────────────────────────────────────────────────────

def render_charts(ws: pd.DataFrame, outflows: pd.DataFrame):

    # Timeline
    if not outflows.empty and "block_time" in outflows.columns:
        st.markdown('<div class="section-title">Timeline de Retiros</div>', unsafe_allow_html=True)

        of = outflows.copy()
        of["date"] = of["block_time"].dt.date

        if "segment" in of.columns:
            daily = of.groupby(["date", "segment"])["value_usd"].sum().reset_index()
        else:
            daily = of.groupby("date")["value_usd"].sum().reset_index().rename(columns={"value_usd": "value_usd"})
            daily["segment"] = "total"

        daily_total = of.groupby("date")["value_usd"].sum().reset_index()
        daily_total["cumulative"] = daily_total["value_usd"].cumsum()

        fig = go.Figure()
        for seg, color in [("preferente", C_BLUE), ("retail", C_ORANGE)]:
            d = daily[daily["segment"] == seg] if "segment" in daily.columns else pd.DataFrame()
            if not d.empty:
                fig.add_trace(go.Bar(
                    x=d["date"], y=d["value_usd"],
                    name=seg.capitalize(), marker_color=color,
                    opacity=0.8, marker_line_width=0,
                ))

        fig.add_trace(go.Scatter(
            x=daily_total["date"], y=daily_total["cumulative"],
            name="Acumulado", yaxis="y2",
            line=dict(color="#CDD5E0", width=2),
            mode="lines",
        ))

        fig.update_layout(
            **PLOTLY_BASE,
            barmode="stack", height=280,
            yaxis=dict(title="USD / día", gridcolor=C_GRID, tickformat="$,.0f", zeroline=False),
            yaxis2=dict(title="Acumulado USD", overlaying="y", side="right",
                        gridcolor=C_GRID, tickformat="$,.0f", zeroline=False),
            legend=dict(orientation="h", y=1.08, x=0, bgcolor="rgba(0,0,0,0)", font_size=11),
            xaxis=dict(gridcolor=C_GRID, showline=False),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Top wallets + Status breakdown
    st.markdown('<div class="section-title">Saldos y Estado de Wallets</div>', unsafe_allow_html=True)

    c1, c2 = st.columns([5, 2])

    with c1:
        top = ws[ws["total_balance_usd"] > 1].nlargest(20, "total_balance_usd").copy()
        top["label"] = top.apply(
            lambda r: (str(r.get("customer_name") or "").strip() or r["wallet_address"][:10] + "…"), axis=1
        )
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=top["total_balance_usd"], y=top["label"], orientation="h",
            name="Remanente", marker_color=C_GREEN, marker_line_width=0, opacity=0.9,
            text=top["total_balance_usd"].apply(lambda v: f"  {fmt(v)}"),
            textposition="inside", insidetextanchor="start",
            textfont=dict(color="#0D1117", size=11, family="Inter, monospace"),
        ))
        fig2.add_trace(go.Bar(
            x=top["total_outflow_usd"], y=top["label"], orientation="h",
            name="Retirado", marker_color=C_RED, marker_line_width=0, opacity=0.35,
        ))
        fig2.update_layout(
            **PLOTLY_BASE,
            barmode="stack", height=520,
            yaxis=dict(autorange="reversed", showgrid=False, tickfont=dict(size=11)),
            xaxis=dict(tickformat="$,.0f", gridcolor=C_GRID, zeroline=False),
            legend=dict(orientation="h", y=1.04, x=0, bgcolor="rgba(0,0,0,0)", font_size=11),
        )
        st.plotly_chart(fig2, use_container_width=True)

    with c2:
        st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)

        # Status breakdown cards
        statuses = [
            ("Retirado completamente",     C_RED,      "Retirado"),
            ("Retiro parcial",             "#FBBF24",  "Parcial"),
            ("Sin movimiento",             C_DIM,      "Sin mov."),
            ("Sin saldo (sin retiro detectado)", "#1C2333", "Sin datos"),
        ]
        counts = ws["status"].value_counts()
        total = len(ws)

        for key, color, label in statuses:
            n = counts.get(key, 0)
            p = pct(n, total)
            html = (
                f"<div class='card-inner' style='margin-bottom:8px'>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:6px'>"
                f"<span style='font-size:11px;font-weight:600;color:{color};letter-spacing:0.06em;text-transform:uppercase'>{label}</span>"
                f"<span style='font-size:18px;font-weight:700;color:#CDD5E0'>{n}</span>"
                f"</div>"
                f"<div class='track'><div style='background:{color};width:{p:.1f}%;height:100%;border-radius:4px'></div></div>"
                f"<div style='font-size:11px;color:#4B5675;margin-top:4px'>{p:.0f}% de {total:,} wallets</div>"
                f"</div>"
            )
            st.markdown(html, unsafe_allow_html=True)

        # Token price reference
        data = load_data()
        pools = data.get("pools", pd.DataFrame())
        if not pools.empty and "token" in pools.columns and "price" in pools.columns:
            arch_prices = pools[pools["token"] != "USDC"][["token", "price"]].drop_duplicates("token")
            if not arch_prices.empty:
                st.markdown("<div style='margin-top:16px'></div>", unsafe_allow_html=True)
                st.markdown('<div class="section-title" style="margin-top:0">Precios actuales</div>', unsafe_allow_html=True)
                for _, row in arch_prices.sort_values("price", ascending=False).iterrows():
                    html = (f"<div style='display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #111827'>"
                            f"<span style='font-size:12px;color:#4B5675'>{row['token']}</span>"
                            f"<span style='font-size:12px;font-weight:600;color:#9CA3AF'>${row['price']:,.4f}</span>"
                            f"</div>")
                    st.markdown(html, unsafe_allow_html=True)


# ─── TOKENS ───────────────────────────────────────────────────────────────────

# AP60 es versión legacy de AMOD → se agrupa bajo AMOD
AMOD_GROUP = {"AMOD", "AP60"}

TOKEN_META = {
    # base_symbol → (display_name, accent_color, has_price)
    "AEDY":  ("AEDY",  "#6366F1", True),
    "ABDY":  ("ABDY",  "#8B5CF6", True),
    "CHAIN": ("CHAIN", "#3B82F6", True),
    "WEB3":  ("WEB3",  "#10B981", True),
    "ADDY":  ("ADDY",  "#F59E0B", True),
    "ACAI":  ("ACAI",  "#EC4899", True),
    "AAGG":  ("AAGG",  "#475569", False),
    "AMOD":  ("AMOD / AP60", "#475569", False),
    "ABAL":  ("ABAL",  "#475569", False),
}


def render_tokens(balances: pd.DataFrame, outflows: pd.DataFrame):
    if balances.empty:
        return

    st.markdown('<div class="section-title">Desglose por Token</div>', unsafe_allow_html=True)

    # ── Normalise: colapsar AP60 → AMOD ────────────────────────────────────────
    bal = balances.copy()
    out = outflows.copy() if not outflows.empty else pd.DataFrame()
    bal["base_symbol"] = bal["base_symbol"].replace("AP60", "AMOD")
    if not out.empty and "base_symbol" in out.columns:
        out["base_symbol"] = out["base_symbol"].replace("AP60", "AMOD")

    # ── Aggregate by base ───────────────────────────────────────────────────────
    bal_agg = bal.groupby("base_symbol").agg(
        remaining_usd=("value_usd", "sum"),
        balance_tokens=("balance", "sum"),
        holders=("wallet_address", "nunique"),
        price_usd=("price_usd", "first"),
    ).reset_index()

    out_agg = (out.groupby("base_symbol").agg(
        withdrawn_usd=("value_usd", "sum"),
        withdrawn_tokens=("amount", "sum"),
        outflow_events=("amount", "count"),
    ).reset_index() if not out.empty else pd.DataFrame(columns=["base_symbol","withdrawn_usd","withdrawn_tokens","outflow_events"]))

    tok = bal_agg.merge(out_agg, on="base_symbol", how="outer").fillna(0)
    tok["initial_usd"] = tok["remaining_usd"] + tok["withdrawn_usd"]
    tok["pct_wdn"] = tok.apply(lambda r: r["withdrawn_usd"] / r["initial_usd"] * 100 if r["initial_usd"] > 0 else 0, axis=1)

    # Split priced vs vault
    priced = tok[tok["price_usd"] > 0].sort_values("initial_usd", ascending=False)
    vault  = tok[tok["price_usd"] == 0].sort_values("balance_tokens", ascending=False)

    # ── Bar chart: priced tokens ────────────────────────────────────────────────
    if not priced.empty:
        fig = go.Figure()
        colors = [TOKEN_META.get(s, ("", "#6B7A99", True))[1] for s in priced["base_symbol"]]

        fig.add_trace(go.Bar(
            name="Remanente",
            x=priced["base_symbol"], y=priced["remaining_usd"],
            marker_color=colors, marker_line_width=0, opacity=0.85,
            text=priced["remaining_usd"].apply(lambda v: fmt(v) if v > 0 else ""),
            textposition="inside", insidetextanchor="middle",
            textfont=dict(size=11, color="#0D1117"),
        ))
        fig.add_trace(go.Bar(
            name="Retirado",
            x=priced["base_symbol"], y=priced["withdrawn_usd"],
            marker_color="#F87171", marker_line_width=0, opacity=0.3,
        ))
        fig.update_layout(
            **PLOTLY_BASE,
            barmode="stack", height=240,
            yaxis=dict(tickformat="$,.0f", gridcolor=C_GRID, zeroline=False),
            xaxis=dict(showgrid=False),
            legend=dict(orientation="h", y=1.1, x=0, bgcolor="rgba(0,0,0,0)", font_size=11),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Token cards grid ────────────────────────────────────────────────────────
    # Priced tokens row
    n_priced = len(priced)
    if n_priced:
        cols = st.columns(n_priced)
        for i, (_, row) in enumerate(priced.iterrows()):
            sym = row["base_symbol"]
            name, color, _ = TOKEN_META.get(sym, (sym, "#6B7A99", True))
            pct_w = row["pct_wdn"]
            pct_r = 100 - pct_w
            with cols[i]:
                html = (
                    f"<div class='card' style='padding:14px 16px'>"
                    f"<div style='font-size:13px;font-weight:700;color:{color};letter-spacing:0.04em;margin-bottom:10px'>{name}</div>"
                    f"<div style='font-size:10px;color:#4B5675;text-transform:uppercase;letter-spacing:0.06em'>Remanente</div>"
                    f"<div style='font-size:18px;font-weight:700;color:#3DD68C;letter-spacing:-0.01em'>{fmt(row['remaining_usd'])}</div>"
                    f"<div style='font-size:10px;color:#4B5675;margin-top:8px;text-transform:uppercase;letter-spacing:0.06em'>Retirado</div>"
                    f"<div style='font-size:14px;font-weight:600;color:#F87171'>{fmt(row['withdrawn_usd'])}</div>"
                    f"<div class='track' style='margin-top:10px'><div style='background:#F87171;width:{pct_w:.1f}%;height:100%;border-radius:4px'></div></div>"
                    f"<div style='display:flex;justify-content:space-between;margin-top:4px'>"
                    f"<span style='font-size:10px;color:#4B5675'>{pct_w:.0f}% retirado</span>"
                    f"<span style='font-size:10px;color:#4B5675'>{int(row['holders'])} holders</span>"
                    f"</div>"
                    f"<div style='font-size:10px;color:#2D3650;margin-top:6px'>${row['price_usd']:.4f} / token</div>"
                    f"</div>"
                )
                st.markdown(html, unsafe_allow_html=True)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Vault tokens row
    if not vault.empty:
        n_vault = len(vault)
        cols2 = st.columns(n_vault)
        for i, (_, row) in enumerate(vault.iterrows()):
            sym = row["base_symbol"]
            name, color, _ = TOKEN_META.get(sym, (sym, "#475569", False))
            rem_tok = row["balance_tokens"]
            wdn_tok = row["withdrawn_tokens"]
            tot_tok = rem_tok + wdn_tok
            pct_w = row["withdrawn_tokens"] / tot_tok * 100 if tot_tok > 0 else 0
            with cols2[i]:
                html = (
                    f"<div class='card' style='padding:14px 16px;border-style:dashed'>"
                    f"<div style='font-size:13px;font-weight:700;color:{color};letter-spacing:0.04em;margin-bottom:4px'>{name}</div>"
                    f"<div style='font-size:10px;color:#2D3650;margin-bottom:10px'>Vault ERC-4626 · sin precio USD</div>"
                    f"<div style='font-size:10px;color:#4B5675;text-transform:uppercase;letter-spacing:0.06em'>Remanente</div>"
                    f"<div style='font-size:15px;font-weight:600;color:#9CA3AF'>{rem_tok:,.1f} <span style='font-size:11px;color:#4B5675'>tokens</span></div>"
                    f"<div style='font-size:10px;color:#4B5675;margin-top:8px;text-transform:uppercase;letter-spacing:0.06em'>Retirado</div>"
                    f"<div style='font-size:13px;font-weight:500;color:#6B7A99'>{wdn_tok:,.1f} <span style='font-size:11px;color:#4B5675'>tokens</span></div>"
                    f"<div class='track' style='margin-top:10px'><div style='background:{color};width:{pct_w:.1f}%;height:100%;border-radius:4px'></div></div>"
                    f"<div style='display:flex;justify-content:space-between;margin-top:4px'>"
                    f"<span style='font-size:10px;color:#2D3650'>{pct_w:.0f}% retirado</span>"
                    f"<span style='font-size:10px;color:#2D3650'>{int(row['holders'])} holders</span>"
                    f"</div>"
                    f"</div>"
                )
                st.markdown(html, unsafe_allow_html=True)

    # ── Detailed variant breakdown ──────────────────────────────────────────────
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">Variantes por Token</div>', unsafe_allow_html=True)

    # Build variant aggregation (original base_symbol, before AP60→AMOD remap)
    # Use bal (already remapped) and group by actual symbol column
    var_bal = bal.groupby(["base_symbol", "symbol"]).agg(
        remaining_usd=("value_usd", "sum"),
        balance_tokens=("balance", "sum"),
        holders=("wallet_address", "nunique"),
        price_usd=("price_usd", "first"),
    ).reset_index()

    var_out = (out.groupby(["base_symbol", "symbol"]).agg(
        withdrawn_usd=("value_usd", "sum"),
        withdrawn_tokens=("amount", "sum"),
        outflow_events=("amount", "count"),
    ).reset_index() if not out.empty and "symbol" in out.columns else pd.DataFrame())

    var = var_bal.merge(var_out, on=["base_symbol","symbol"], how="outer").fillna(0)
    var["initial_usd"] = var["remaining_usd"] + var["withdrawn_usd"]

    # Order base groups same as display order
    base_order = list(priced["base_symbol"]) + list(vault["base_symbol"])

    TD  = "style='padding:8px 12px;border-bottom:1px solid #111827;font-size:12px'"
    TDR = "style='padding:8px 12px;border-bottom:1px solid #111827;font-size:12px;text-align:right;font-variant-numeric:tabular-nums'"
    TH  = "style='padding:8px 12px;font-size:10px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#4B5675;border-bottom:1px solid #1C2333'"

    for base in base_order:
        name, color, has_p = TOKEN_META.get(base, (base, "#6B7A99", True))
        variants = var[var["base_symbol"] == base].sort_values("remaining_usd" if has_p else "balance_tokens", ascending=False)
        if variants.empty:
            continue

        with st.expander(f"{name}  —  {len(variants)} variante{'s' if len(variants) > 1 else ''}", expanded=(len(base_order) <= 4)):
            if has_p:
                table_html = (
                    f"<table style='width:100%;border-collapse:collapse'>"
                    f"<thead><tr>"
                    f"<th {TH}>Símbolo</th>"
                    f"<th {TH} style='text-align:right'>Remanente</th>"
                    f"<th {TH} style='text-align:right'>Retirado</th>"
                    f"<th {TH} style='text-align:right'>Capital inicial</th>"
                    f"<th {TH} style='text-align:right'>% Retirado</th>"
                    f"<th {TH} style='text-align:right'>Holders</th>"
                    f"<th {TH} style='text-align:right'>Txs</th>"
                    f"</tr></thead><tbody>"
                )
                for _, v in variants.iterrows():
                    pct_w_v = v["withdrawn_usd"] / v["initial_usd"] * 100 if v["initial_usd"] > 0 else 0
                    pct_color = "#F87171" if pct_w_v > 70 else "#FBBF24" if pct_w_v > 30 else "#3DD68C"
                    bar = f"<div style='display:inline-block;width:{pct_w_v:.0f}px;max-width:80px;height:4px;background:{pct_color};border-radius:2px;vertical-align:middle;margin-right:6px'></div>"
                    table_html += (
                        f"<tr>"
                        f"<td {TD}><span style='font-family:monospace;font-size:11px;color:{color};font-weight:600'>{v['symbol']}</span></td>"
                        f"<td {TDR} style='color:#3DD68C'>{fmt(v['remaining_usd'])}</td>"
                        f"<td {TDR} style='color:#F87171'>{fmt(v['withdrawn_usd'])}</td>"
                        f"<td {TDR}>{fmt(v['initial_usd'])}</td>"
                        f"<td {TDR}>{bar}<span style='font-size:11px;color:#6B7A99'>{pct_w_v:.1f}%</span></td>"
                        f"<td {TDR}>{int(v['holders']):,}</td>"
                        f"<td {TDR}>{int(v['outflow_events']):,}</td>"
                        f"</tr>"
                    )
                table_html += "</tbody></table>"
            else:
                # Vault: show tokens not USD
                table_html = (
                    f"<table style='width:100%;border-collapse:collapse'>"
                    f"<thead><tr>"
                    f"<th {TH}>Símbolo</th>"
                    f"<th {TH} style='text-align:right'>Remanente (tokens)</th>"
                    f"<th {TH} style='text-align:right'>Retirado (tokens)</th>"
                    f"<th {TH} style='text-align:right'>% Retirado</th>"
                    f"<th {TH} style='text-align:right'>Holders</th>"
                    f"<th {TH} style='text-align:right'>Txs</th>"
                    f"</tr></thead><tbody>"
                )
                for _, v in variants.iterrows():
                    tot_v = v["balance_tokens"] + v["withdrawn_tokens"]
                    pct_w_v = v["withdrawn_tokens"] / tot_v * 100 if tot_v > 0 else 0
                    table_html += (
                        f"<tr>"
                        f"<td {TD}><span style='font-family:monospace;font-size:11px;color:{color};font-weight:600'>{v['symbol']}</span></td>"
                        f"<td {TDR}>{v['balance_tokens']:,.2f}</td>"
                        f"<td {TDR}>{v['withdrawn_tokens']:,.2f}</td>"
                        f"<td {TDR} style='color:#4B5675'>{pct_w_v:.1f}%</td>"
                        f"<td {TDR}>{int(v['holders']):,}</td>"
                        f"<td {TDR}>{int(v['outflow_events']):,}</td>"
                        f"</tr>"
                    )
                table_html += "</tbody></table>"
                table_html += "<div style='font-size:11px;color:#2D3650;margin-top:8px;padding:8px 12px'>⚠️ Vault ERC-4626 — precio NAV no disponible, se muestra en tokens.</div>"

            st.markdown(table_html, unsafe_allow_html=True)


# ─── TABLES ───────────────────────────────────────────────────────────────────

def render_tables(ws: pd.DataFrame, balances: pd.DataFrame, outflows: pd.DataFrame, filters: dict):
    st.markdown('<div class="section-title">Tablas de Detalle</div>', unsafe_allow_html=True)

    data = load_data()
    unknown = data.get("unknown_wallets", pd.DataFrame())
    n_unk = len(unknown[unknown["total_aum"] > 100]) if not unknown.empty else 0
    unk_label = f"⚠️ Sin registrar ({n_unk})" if n_unk > 0 else "Sin registrar"

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "Wallets", "Balances", "Retiros", "Análisis", "Pools", unk_label, "⚖️ Reconciliación", "📋 Direcciones",
    ])

    fmt_style = {"total_balance_usd": "${:,.2f}", "total_outflow_usd": "${:,.2f}", "pct_withdrawn": "{:.1f}%"}

    with tab1:
        if not ws.empty:
            display = ws.copy()
            display["aum_historico_usd"] = display["total_balance_usd"] + display["total_outflow_usd"]

            # ── Pivot de balances: una columna por token ──────────────────────
            bal_data = data.get("balances", pd.DataFrame())
            token_cols = []
            if not bal_data.empty and "base_symbol" in bal_data.columns:
                # Normalizar AP60 → AMOD
                bal_piv = bal_data.copy()
                bal_piv["base_symbol"] = bal_piv["base_symbol"].replace("AP60", "AMOD")

                # Pivot: filas = wallet, columnas = token, valores = balance (tokens) y USD
                piv_usd = bal_piv.pivot_table(
                    index="wallet_address", columns="base_symbol",
                    values="value_usd", aggfunc="sum", fill_value=0
                )
                piv_tok = bal_piv.pivot_table(
                    index="wallet_address", columns="base_symbol",
                    values="balance", aggfunc="sum", fill_value=0
                )

                # Renombrar columnas: "AAGG_usd", "AAGG_qty"
                piv_usd.columns = [f"{c}_usd" for c in piv_usd.columns]
                piv_tok.columns = [f"{c}_qty" for c in piv_tok.columns]
                piv = piv_usd.join(piv_tok).reset_index()

                # Intercalar columnas: AAGG_qty, AAGG_usd, AEDY_qty, AEDY_usd...
                tokens_sorted = sorted(bal_piv["base_symbol"].unique())
                interleaved = []
                for t in tokens_sorted:
                    if f"{t}_qty" in piv.columns: interleaved.append(f"{t}_qty")
                    if f"{t}_usd" in piv.columns: interleaved.append(f"{t}_usd")
                token_cols = interleaved

                display = display.merge(piv[["wallet_address"] + token_cols],
                                        on="wallet_address", how="left")
                for c in token_cols:
                    display[c] = display[c].fillna(0)

            base_cols = ["customer_name", "email", "segment", "network", "status",
                         "aum_historico_usd", "total_balance_usd", "total_outflow_usd",
                         "pct_withdrawn", "num_outflow_events", "last_outflow_date", "wallet_address"]
            all_cols = [c for c in base_cols if c in display.columns] + token_cols
            display = display[all_cols]

            # ── Fila de totales ───────────────────────────────────────────────
            numeric_sum_cols = ["aum_historico_usd", "total_balance_usd", "total_outflow_usd",
                                 "num_outflow_events"] + token_cols
            total_row = {c: "" for c in all_cols}
            total_row["customer_name"] = "TOTAL"
            for c in numeric_sum_cols:
                if c in display.columns:
                    total_row[c] = display[c].sum()
            total_row["pct_withdrawn"] = (
                display["total_outflow_usd"].sum() /
                max(display["aum_historico_usd"].sum(), 1) * 100
            )
            display_with_total = pd.concat(
                [display, pd.DataFrame([total_row])], ignore_index=True
            )

            # ── Formato ───────────────────────────────────────────────────────
            fmt_map = {**fmt_style, "aum_historico_usd": "${:,.2f}"}
            for c in token_cols:
                if c.endswith("_usd"):
                    fmt_map[c] = "${:,.2f}"
                else:
                    fmt_map[c] = "{:,.4f}"

            st.dataframe(display_with_total.style.format(fmt_map, na_rep=""),
                         use_container_width=True, height=560)
            st.download_button("Descargar CSV", display.to_csv(index=False), "wallets.csv", "text/csv")

    with tab2:
        if not balances.empty:
            bf = apply_filters(balances, filters)
            cols = ["customer_name", "segment", "network", "symbol", "balance", "price_usd", "value_usd", "wallet_address"]
            av = [c for c in cols if c in bf.columns]
            st.dataframe(
                bf[av].sort_values("value_usd", ascending=False).style.format(
                    {"balance": "{:,.4f}", "price_usd": "${:,.4f}", "value_usd": "${:,.2f}"}
                ), use_container_width=True, height=540,
            )
        else:
            st.info("Sin datos")

    with tab3:
        if not outflows.empty:
            of = apply_filters(outflows, filters)
            cols = ["customer_name", "segment", "symbol", "amount", "value_usd",
                    "destination_type", "block_time", "wallet_from", "tx_hash"]
            av = [c for c in cols if c in of.columns]
            st.dataframe(
                of[av].sort_values("block_time", ascending=False).style.format(
                    {"amount": "{:,.4f}", "value_usd": "${:,.2f}"}
                ), use_container_width=True, height=540,
            )
        else:
            st.info("Sin datos")

    with tab4:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Mayor saldo remanente — Top 25**")
            st.dataframe(
                ws.nlargest(25, "total_balance_usd")[
                    ["customer_name", "email", "segment", "total_balance_usd", "pct_withdrawn", "status"]
                ].style.format(fmt_style), use_container_width=True,
            )
            st.markdown("**Sin movimiento · saldo > $500**")
            no_move = ws[(ws["status"] == "Sin movimiento") & (ws["total_balance_usd"] > 500)]
            st.dataframe(
                no_move.nlargest(25, "total_balance_usd")[
                    ["customer_name", "email", "segment", "total_balance_usd"]
                ].style.format({"total_balance_usd": "${:,.2f}"}), use_container_width=True,
            )
        with c2:
            st.markdown("**Mayor retiro acumulado — Top 25**")
            st.dataframe(
                ws.nlargest(25, "total_outflow_usd")[
                    ["customer_name", "email", "segment", "total_outflow_usd", "total_balance_usd", "status"]
                ].style.format({"total_outflow_usd": "${:,.2f}", "total_balance_usd": "${:,.2f}"}),
                use_container_width=True,
            )
            st.markdown("**Retiro parcial — pendientes**")
            partial = ws[ws["status"] == "Retiro parcial"].sort_values("total_balance_usd", ascending=False)
            st.dataframe(
                partial[["customer_name", "email", "segment", "total_balance_usd",
                          "total_outflow_usd", "pct_withdrawn", "last_outflow_date"]].style.format({
                    "total_balance_usd": "${:,.2f}", "total_outflow_usd": "${:,.2f}", "pct_withdrawn": "{:.1f}%"
                }), use_container_width=True,
            )

    with tab6:
        if not unknown.empty:
            rem_unk = unknown["balance_usd"].sum()
            out_unk = unknown["outflow_usd"].sum()
            total_unk = rem_unk + out_unk

            m1, m2, m3 = st.columns(3)
            m1.metric("Wallets sin registrar", f"{len(unknown)}", help="Con más de $10 en movimientos")
            m2.metric("Remanente", f"${rem_unk:,.0f}", help="Aún en sus wallets")
            m3.metric("Retirado", f"${out_unk:,.0f}", help="Ya salió de la plataforma")

            st.markdown("---")
            st.markdown(
                "Estas wallets tienen tokens Arch on-chain **pero no están en los CSVs de clientes**. "
                "Pueden ser: clientes con otra wallet, wallets internas no registradas, o contratos desconocidos. "
                "Investigar las de mayor valor."
            )

            show_cols = [c for c in ["wallet_address", "balance_usd", "outflow_usd", "total_aum", "tokens", "last_activity", "num_txs"] if c in unknown.columns]
            st.dataframe(
                unknown[show_cols].style.format({
                    "balance_usd": "${:,.2f}",
                    "outflow_usd": "${:,.2f}",
                    "total_aum":   "${:,.2f}",
                }),
                use_container_width=True, height=500,
            )
            st.download_button(
                "Descargar CSV de wallets desconocidas",
                unknown[show_cols].to_csv(index=False),
                "unknown_wallets.csv", "text/csv",
            )
        else:
            st.info("No se detectaron wallets desconocidas con saldo relevante.")

    with tab5:
        data = load_data()
        pools = data.get("pools", pd.DataFrame())
        if not pools.empty:
            st.markdown("**Liquidez por pool**")
            if "token" in pools.columns and "price" in pools.columns:
                arch = pools[pools["token"] != "USDC"].copy()
                usdc = pools[pools["token"] == "USDC"].copy()
                if not arch.empty and not usdc.empty:
                    merged = arch.merge(
                        usdc[["archemist", "usdBalance"]].rename(columns={"usdBalance": "usd_usdc_side"}),
                        on="archemist", how="left"
                    )
                    merged["liquidez_total"] = merged["usdBalance"].fillna(0) + merged["usd_usdc_side"].fillna(0)
                    show = [c for c in ["archemist", "token", "price", "usdBalance", "usd_usdc_side", "liquidez_total"] if c in merged.columns]
                    st.dataframe(
                        merged[show].sort_values("liquidez_total", ascending=False).style.format(
                            {"price": "${:,.4f}", "usdBalance": "${:,.2f}", "usd_usdc_side": "${:,.2f}", "liquidez_total": "${:,.2f}"}
                        ), use_container_width=True,
                    )
                else:
                    st.dataframe(pools, use_container_width=True)

        supply = data.get("supply", pd.DataFrame())
        if not supply.empty and "day" in supply.columns:
            st.markdown("**Supply desde 1 Abr 2026**")
            supply["day"] = pd.to_datetime(supply["day"], errors="coerce")
            st.dataframe(supply[supply["day"] >= pd.Timestamp("2026-04-01")], use_container_width=True)

    with tab7:
        data = load_data()
        rec  = data.get("reconciliation", pd.DataFrame())
        ws   = data.get("wallet_summary", pd.DataFrame())

        if rec.empty:
            st.info("Sin datos. Ejecutá el pipeline primero.")
        else:
            # ── Totales (deben calzar exactamente con el overview) ────────────
            total_rem  = rec["remanente_usd"].sum() if "remanente_usd" in rec.columns else 0
            total_ret  = rec["retirado_usd"].sum()  if "retirado_usd"  in rec.columns else 0
            total_hist = rec["historico_usd"].sum()  if "historico_usd" in rec.columns else 0
            n_warn     = rec["alerta"].str.startswith("⚠️").sum()

            # Verificar que calzan con overview
            ws_rem = ws["total_balance_usd"].sum() if not ws.empty else 0
            ws_ret = ws["total_outflow_usd"].sum()  if not ws.empty else 0
            delta_rem = abs(total_rem - ws_rem)
            delta_ret = abs(total_ret - ws_ret)

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Remanente clientes", f"${total_rem:,.0f}",
                      help="Suma de saldos actuales — debe calzar con el overview")
            m2.metric("Retirado desde Apr 1", f"${total_ret:,.0f}",
                      help="Suma de retiros reales (excluye transferencias internas)")
            m3.metric("AUM histórico estimado", f"${total_hist:,.0f}",
                      help="Remanente + Retirado = lo que tenían los clientes al inicio del cierre")
            m4.metric("Estado", "✓ Cuadra" if n_warn == 0 else f"⚠️ {n_warn} alertas")

            # Validación de consistencia con overview
            if delta_rem < 100 and delta_ret < 100:
                st.success(f"✓ Números consistentes con el overview — Remanente ${total_rem:,.0f} | Retirado ${total_ret:,.0f}")
            else:
                st.warning(
                    f"⚠️ Pequeña diferencia con el overview: "
                    f"remanente Δ${delta_rem:,.0f} | retirado Δ${delta_ret:,.0f}. "
                    "Puede deberse a redondeo o wallets sin precio."
                )

            # Barra: remanente vs retirado
            pct_ret = total_ret / max(total_hist, 1) * 100
            pct_rem = 100 - pct_ret
            st.markdown(
                f"<div style='margin:16px 0 4px 0'>"
                f"<div style='display:flex;gap:0;height:8px;border-radius:4px;overflow:hidden;background:#1C2333'>"
                f"<div style='width:{pct_ret:.1f}%;background:#F87171'></div>"
                f"<div style='width:{pct_rem:.1f}%;background:#3DD68C'></div>"
                f"</div>"
                f"<div style='display:flex;gap:16px;margin-top:6px;font-size:11px;color:#4B5675'>"
                f"<span><span style='color:#F87171'>●</span> Retirado {pct_ret:.1f}%</span>"
                f"<span><span style='color:#3DD68C'>●</span> Remanente {pct_rem:.1f}%</span>"
                f"</div></div>",
                unsafe_allow_html=True,
            )

            st.markdown("---")

            # ── Vista de Calce ────────────────────────────────────────────────
            st.markdown(
                "<div style='font-size:10px;font-weight:700;letter-spacing:0.1em;"
                "text-transform:uppercase;color:#4B5675;margin-bottom:16px'>"
                "Vista de Calce — Market Cap vs Remanente Total</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                "<div style='font-size:11px;color:#4B5675;margin-bottom:16px;line-height:1.6'>"
                "Verificá si: <b style='color:#9CA3AF'>Market Cap tokens ETH + ABDY_V1 (POL)</b> "
                "= <b style='color:#3DD68C'>Total remanente clientes</b>. "
                "Usá los filtros para ver solo ETH, solo Polygon, o tokens individuales."
                "</div>",
                unsafe_allow_html=True,
            )

            # Clasificar tokens por red de supply para el calce:
            # - "ETH"  → tiene supply en Ethereum (usamos market_cap_eth para el calce)
            # - "POL"  → solo existe en Polygon, sin supply ETH (usamos market_cap_pol)
            # Tokens con supply en ambas redes se clasifican como "ETH" porque la fórmula
            # de calce usa solo el market cap ETH (el lado POL son los _PROD wrappers).
            rec_c = rec.copy()
            for _col in ["supply_eth", "supply_pol", "market_cap_eth", "market_cap_pol", "market_cap_usd"]:
                if _col not in rec_c.columns:
                    rec_c[_col] = 0.0
                rec_c[_col] = rec_c[_col].fillna(0.0)

            def _tok_net(row):
                if row["supply_eth"] > 0: return "ETH"
                if row["supply_pol"] > 0: return "POL"
                return "—"
            rec_c["_network"] = rec_c.apply(_tok_net, axis=1)

            # Default del calce:
            #   - Tokens con supply_eth > 0 → usan market_cap_eth
            #     (incluye CHAIN_PROD si es el portador único del supply ETH de CHAIN)
            #   - ABDY_V1 → usa market_cap_pol (token legacy polygon, no es wrapper de ABDY)
            #   - _PROD con supply_eth = 0 → EXCLUIDOS (son wrappers polygon del base ETH;
            #     sumarlos sería doble-contar la misma exposición económica)
            #   - Vault tokens (AAGG/AMOD/ABAL/AP60) → EXCLUIDOS del calce base
            _eth_supply_toks = set(rec_c[rec_c["supply_eth"] > 0]["token"].tolist())
            tok_opts_c = rec_c["token"].tolist()
            default_toks_c = [
                t for t in tok_opts_c
                if t in _eth_supply_toks or t == "ABDY_V1"
            ]

            # Filtros de la vista de calce
            cf1, cf2 = st.columns([1, 3])
            with cf1:
                net_opts_c = [n for n in ["ETH", "POL"] if n in rec_c["_network"].values]
                sel_nets_c = st.multiselect(
                    "Red de supply", net_opts_c, default=net_opts_c,
                    key="calce_net",
                    help="ETH = market cap en Ethereum (supply_eth × precio) | POL = solo tokens que únicamente existen en Polygon (ABDY_V1, vaults)",
                )
            with cf2:
                sel_toks_c = st.multiselect(
                    "Tokens", tok_opts_c, default=default_toks_c,
                    key="calce_tok",
                    help="Por defecto: tokens ETH (supply_eth > 0) + ABDY_V1. Los _PROD wrappers y vaults están excluidos — agregarlos manualmente si querés verlos.",
                )

            _nets_active  = sel_nets_c if sel_nets_c else net_opts_c
            _toks_active  = sel_toks_c if sel_toks_c else default_toks_c
            calce_df = rec_c[
                rec_c["_network"].isin(_nets_active) &
                rec_c["token"].isin(_toks_active)
            ].copy()

            # Contribución de market cap según la fórmula de calce:
            # - Si tiene supply ETH → usar market_cap_eth (aunque también tenga supply POL)
            # - Si solo tiene supply POL → usar market_cap_pol (ej. ABDY_V1, vaults)
            def _cap_contrib(row):
                if row["supply_eth"] > 0:
                    return row["market_cap_eth"]
                return row["market_cap_pol"]

            calce_df["_market_cap_red"] = calce_df.apply(_cap_contrib, axis=1)

            # Supply que corresponde a la red del calce
            def _sup_red(row):
                if row["supply_eth"] > 0: return row["supply_eth"]
                return row["supply_pol"]
            calce_df["_supply_red"] = calce_df.apply(_sup_red, axis=1)

            # Métricas de calce
            total_rem_ws   = ws["total_balance_usd"].sum() if not ws.empty else rec["remanente_usd"].sum()
            sum_cap_calce  = calce_df["_market_cap_red"].sum()
            sum_rem_calce  = calce_df["remanente_usd"].sum()
            delta_calce    = sum_cap_calce - total_rem_ws
            delta_pct_c    = delta_calce / max(sum_cap_calce, 1) * 100
            pct_rem_cap    = sum_rem_calce / max(sum_cap_calce, 1) * 100
            calce_ok_c     = abs(delta_pct_c) < 5  # tolerancia 5%

            cc1, cc2, cc3, cc4 = st.columns(4)
            cc1.metric("Market Cap filtrado", f"${sum_cap_calce:,.0f}",
                       help="Supply en la red seleccionada × precio")
            cc2.metric("Remanente total clientes", f"${total_rem_ws:,.0f}",
                       help="Total del overview — todos los segmentos")
            cc3.metric("Δ Cap − Remanente", f"${delta_calce:+,.0f}",
                       delta=f"{delta_pct_c:+.1f}%",
                       delta_color="normal" if abs(delta_pct_c) < 5 else "inverse")
            cc4.metric("Calce", "✓ Cuadra" if calce_ok_c else "⚠️ Revisar",
                       delta=f"{pct_rem_cap:.1f}% del cap cubierto")

            # Tabla de calce
            CTH  = "style='padding:7px 10px;font-size:10px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#4B5675;border-bottom:1px solid #1C2333;text-align:right'"
            CTHL = "style='padding:7px 10px;font-size:10px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#4B5675;border-bottom:1px solid #1C2333'"
            CTD  = "style='padding:7px 10px;border-bottom:1px solid #111827;font-size:12px'"
            CTDR = "style='padding:7px 10px;border-bottom:1px solid #111827;font-size:12px;text-align:right;font-variant-numeric:tabular-nums'"

            ctable = (
                "<table style='width:100%;border-collapse:collapse;margin-top:12px'>"
                "<thead><tr>"
                f"<th {CTHL}>Token</th>"
                f"<th {CTH}>Red</th>"
                f"<th {CTH}>Supply en red</th>"
                f"<th {CTH}>Precio USD</th>"
                f"<th {CTH}>Market Cap</th>"
                "</tr></thead><tbody>"
            )

            for _, crow in calce_df.iterrows():
                net_color = "#3B82F6" if crow["_network"] == "ETH" else "#8B5CF6" if crow["_network"] == "POL" else "#9CA3AF"
                sup_v = crow["_supply_red"]
                cap_v = crow["_market_cap_red"]
                prc_v = crow.get("precio_usd", 0)
                ctable += (
                    f"<tr>"
                    f"<td {CTD}><span style='font-family:monospace;font-weight:700;color:#CDD5E0'>{crow['token']}</span></td>"
                    f"<td {CTDR}><span style='color:{net_color};font-weight:600;font-size:11px;letter-spacing:0.04em'>{crow['_network']}</span></td>"
                    f"<td {CTDR} style='color:#6B7A99'>{sup_v:,.1f}</td>"
                    f"<td {CTDR} style='color:#9CA3AF'>${prc_v:,.4f}</td>"
                    f"<td {CTDR} style='color:#CDD5E0;font-weight:600'>${cap_v:,.0f}</td>"
                    f"</tr>"
                )

            # Fila total calce
            CTF  = "style='padding:8px 10px;font-size:12px;font-weight:700;color:#CDD5E0;text-align:right;border-top:2px solid #2D3650;background:#111827'"
            CTFL = "style='padding:8px 10px;font-size:12px;font-weight:700;color:#CDD5E0;border-top:2px solid #2D3650;background:#111827'"
            ctable += (
                f"<tfoot><tr>"
                f"<td {CTFL}>TOTAL</td>"
                f"<td {CTF}>—</td>"
                f"<td {CTF}>—</td>"
                f"<td {CTF}>—</td>"
                f"<td style='padding:8px 10px;font-size:12px;font-weight:700;color:#CDD5E0;text-align:right;border-top:2px solid #2D3650;background:#111827'>${sum_cap_calce:,.0f}</td>"
                f"</tr></tfoot>"
            )
            ctable += "</tbody></table>"
            st.markdown(ctable, unsafe_allow_html=True)

            st.markdown(
                "<div style='font-size:11px;color:#2D3650;margin-top:12px;margin-bottom:4px;"
                "padding:8px 12px;border:1px solid #1C2333;border-radius:6px;line-height:1.7'>"
                "ℹ️ <b>Cómo leer este calce:</b> "
                "Market Cap = supply en la red × precio. "
                "Por defecto: tokens con supply en ETH + ABDY_V1 (POL). "
                "Los _PROD wrappers están excluidos — agregarlos manualmente si querés verlos."
                "</div>",
                unsafe_allow_html=True,
            )

            st.markdown("---")

            # ── Detalle Completo por Token ─────────────────────────────────────
            st.markdown(
                "<div style='font-size:10px;font-weight:700;letter-spacing:0.1em;"
                "text-transform:uppercase;color:#4B5675;margin-bottom:12px'>"
                "Detalle Completo por Token</div>",
                unsafe_allow_html=True,
            )

            # ── Tabla por token ───────────────────────────────────────────────
            TH  = "style='padding:8px 12px;font-size:10px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#4B5675;border-bottom:1px solid #1C2333;text-align:right'"
            THL = "style='padding:8px 12px;font-size:10px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#4B5675;border-bottom:1px solid #1C2333'"
            TD  = "style='padding:9px 12px;border-bottom:1px solid #111827;font-size:12px'"
            TDR = "style='padding:9px 12px;border-bottom:1px solid #111827;font-size:12px;text-align:right;font-variant-numeric:tabular-nums'"

            table_html = (
                "<table style='width:100%;border-collapse:collapse'>"
                "<thead><tr>"
                f"<th {THL}>Token</th>"
                f"<th {TH}>Precio</th>"
                f"<th {TH}>Supply ETH<br><span style='font-weight:400;opacity:0.5'>tokens base</span></th>"
                f"<th {TH}>Market Cap ETH</th>"
                f"<th {TH}>Supply POL<br><span style='font-weight:400;opacity:0.5'>portfolios</span></th>"
                f"<th {TH}>Market Cap POL</th>"
                f"<th {TH}>Holders</th>"
                f"<th {TH}>Remanente</th>"
                f"<th {TH}>Retirado</th>"
                f"<th {TH}>AUM histórico</th>"
                f"<th {TH}>% Rem / Cap ETH</th>"
                f"<th {TH}>% retirado</th>"
                f"<th {TH}>Txs</th>"
                f"<th {TH}>Estado</th>"
                "</tr></thead><tbody>"
            )

            for _, row in rec.iterrows():
                alerta       = row["alerta"]
                alerta_color = "#F87171" if alerta.startswith("⚠️") else "#3DD68C"

                pct_r   = row.get("pct_retirado", 0)
                bar_w   = min(pct_r, 100)
                bar_col = "#F87171" if pct_r > 70 else "#FBBF24" if pct_r > 30 else "#3DD68C"
                pct_bar = (
                    f"<div style='display:flex;align-items:center;gap:5px;justify-content:flex-end'>"
                    f"<div style='width:40px;height:4px;background:#1C2333;border-radius:2px;overflow:hidden'>"
                    f"<div style='width:{bar_w:.0f}%;height:100%;background:{bar_col}'></div></div>"
                    f"<span style='color:{bar_col}'>{pct_r:.1f}%</span></div>"
                )

                sup_eth    = row.get("supply_eth", 0)
                sup_pol    = row.get("supply_pol", 0)
                sup_total  = row.get("supply_total", 0)
                cap_eth    = row.get("market_cap_eth", 0)
                cap_pol    = row.get("market_cap_pol", 0)
                cap_total  = row.get("market_cap_usd", 0)
                # Si no hay split ETH/POL todavía, mostrar supply_total en la columna ETH
                if sup_eth == 0 and sup_pol == 0 and sup_total > 0:
                    sup_eth = sup_total
                    cap_eth = cap_total
                ref_cap    = cap_eth if cap_eth > 0 else cap_pol
                pct_vs_cap = row.get("pct_remanente_vs_cap", 0)
                cap_color  = "#F87171" if pct_vs_cap > 105 else "#9CA3AF"

                def fmt_sup(v): return f"{v:,.1f}" if v > 0 else "—"
                def fmt_cap(v): return f"${v:,.0f}" if v > 0 else "—"

                pct_cap_fmt = f"<span style='color:{cap_color}'>{pct_vs_cap:.1f}%</span>" if ref_cap > 0 else "—"

                table_html += (
                    f"<tr>"
                    f"<td {TD}><span style='font-family:monospace;font-weight:700;color:#CDD5E0'>{row['token']}</span></td>"
                    f"<td {TDR} style='color:#9CA3AF'>${row['precio_usd']:,.4f}</td>"
                    f"<td {TDR} style='color:#3B82F6'>{fmt_sup(sup_eth)}</td>"
                    f"<td {TDR} style='color:#3B82F6'>{fmt_cap(cap_eth)}</td>"
                    f"<td {TDR} style='color:#8B5CF6'>{fmt_sup(sup_pol)}</td>"
                    f"<td {TDR} style='color:#8B5CF6'>{fmt_cap(cap_pol)}</td>"
                    f"<td {TDR} style='color:#6B7A99'>{int(row.get('holders', 0)):,}</td>"
                    f"<td {TDR} style='color:#3DD68C'>${row.get('remanente_usd', 0):,.0f}</td>"
                    f"<td {TDR} style='color:#F87171'>${row.get('retirado_usd', 0):,.0f}</td>"
                    f"<td {TDR} style='color:#9CA3AF'>${row.get('historico_usd', 0):,.0f}</td>"
                    f"<td {TDR}>{pct_cap_fmt}</td>"
                    f"<td {TDR}>{pct_bar}</td>"
                    f"<td {TDR} style='color:#4B5675'>{int(row.get('num_txs', 0)):,}</td>"
                    f"<td style='padding:9px 12px;border-bottom:1px solid #111827;font-size:12px;text-align:right;font-weight:600;color:{alerta_color}'>{alerta}</td>"
                    f"</tr>"
                )

            # ── Fila de totales ───────────────────────────────────────────────
            t_rem      = rec["remanente_usd"].sum()
            t_ret      = rec["retirado_usd"].sum()
            t_hist     = rec["historico_usd"].sum()
            t_cap_eth  = rec["market_cap_eth"].sum() if "market_cap_eth" in rec.columns else 0
            t_cap_pol  = rec["market_cap_pol"].sum() if "market_cap_pol" in rec.columns else 0
            t_txs      = int(rec["num_txs"].sum())
            t_hol      = int(rec["holders"].sum())
            t_pct_ret  = t_ret / t_hist * 100 if t_hist > 0 else 0
            t_ref_cap  = t_cap_eth if t_cap_eth > 0 else t_cap_pol
            t_pct_cap  = t_rem / t_ref_cap * 100 if t_ref_cap > 0 else 0
            TF  = "style='padding:10px 12px;font-size:12px;font-weight:700;color:#CDD5E0;text-align:right;border-top:2px solid #2D3650;background:#111827'"
            TFL = "style='padding:10px 12px;font-size:12px;font-weight:700;color:#CDD5E0;border-top:2px solid #2D3650;background:#111827'"
            table_html += (
                f"<tfoot><tr>"
                f"<td {TFL}>TOTAL</td>"
                f"<td {TF}>—</td>"
                f"<td {TF}>—</td>"
                f"<td style='padding:10px 12px;font-size:12px;font-weight:700;color:#3B82F6;text-align:right;border-top:2px solid #2D3650;background:#111827'>${t_cap_eth:,.0f}</td>"
                f"<td {TF}>—</td>"
                f"<td style='padding:10px 12px;font-size:12px;font-weight:700;color:#8B5CF6;text-align:right;border-top:2px solid #2D3650;background:#111827'>${t_cap_pol:,.0f}</td>"
                f"<td {TF}>{t_hol:,}</td>"
                f"<td style='padding:10px 12px;font-size:12px;font-weight:700;color:#3DD68C;text-align:right;border-top:2px solid #2D3650;background:#111827'>${t_rem:,.0f}</td>"
                f"<td style='padding:10px 12px;font-size:12px;font-weight:700;color:#F87171;text-align:right;border-top:2px solid #2D3650;background:#111827'>${t_ret:,.0f}</td>"
                f"<td {TF}>${t_hist:,.0f}</td>"
                f"<td {TF}>{t_pct_cap:.1f}%</td>"
                f"<td {TF}>{t_pct_ret:.1f}%</td>"
                f"<td {TF}>{t_txs:,}</td>"
                f"<td {TF}>—</td>"
                f"</tr></tfoot>"
            )
            table_html += "</tbody></table>"
            st.markdown(table_html, unsafe_allow_html=True)

            st.markdown(
                "<div style='font-size:11px;color:#2D3650;margin-top:16px;padding:10px 14px;"
                "border:1px solid #1C2333;border-radius:6px;line-height:1.7'>"
                "ℹ️ Todos los números usan los mismos datos que el overview — "
                "<b>Remanente</b> = saldo actual de clientes | "
                "<b>Retirado</b> = retiros reales desde el 1 Abr (excluye transferencias entre clientes) | "
                "<b>AUM histórico</b> = Remanente + Retirado. "
                "Las wallets no registradas se muestran por separado en la tab 'Sin registrar'."
                "</div>",
                unsafe_allow_html=True,
            )

            st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
            st.download_button(
                "Descargar CSV",
                rec.to_csv(index=False),
                "reconciliacion.csv",
                "text/csv",
            )


    with tab8:
        TH  = "style='padding:8px 12px;font-size:10px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#4B5675;border-bottom:1px solid #1C2333'"
        TD  = "style='padding:8px 12px;border-bottom:1px solid #111827;font-size:12px;color:#9CA3AF'"
        TDM = "style='padding:8px 12px;border-bottom:1px solid #111827;font-size:12px;color:#CDD5E0;font-weight:600'"
        TDA = "style='padding:8px 12px;border-bottom:1px solid #111827;font-size:11px;font-family:monospace;color:#60A5FA'"

        def scan_link(addr, network="POL"):
            if not addr or addr in ("N/A", ""):
                return "—"
            base = "https://etherscan.io/address/" if network == "ETH" else "https://polygonscan.com/address/"
            short = addr[:10] + "…" + addr[-6:] if len(addr) > 20 else addr
            return f"<a href='{base}{addr}' target='_blank' style='color:#60A5FA;text-decoration:none'>{short}</a>"

        def state_badge(state):
            if "Active" in state or "Active" in state:
                return "<span style='color:#3DD68C'>● Activo</span>"
            elif "Deprecated" in state or "⚰️" in state:
                return "<span style='color:#374151'>● Deprecado</span>"
            elif "Ready" in state or "🚀" in state:
                return "<span style='color:#FBBF24'>● Ready</span>"
            return state

        # ── 0. Balances on-chain de contratos Arch ────────────────────────────
        st.markdown('<div class="section-title">Tokens Arch en contratos (on-chain)</div>', unsafe_allow_html=True)
        cb = data.get("contract_balances", pd.DataFrame())
        if not cb.empty and "value_usd" in cb.columns:
            cb_f = cb[cb["value_usd"] > 0.01].copy()

            # Pivot USD
            piv_usd = cb_f.pivot_table(
                index=["wallet", "contract_name", "network"],
                columns="base_symbol", values="value_usd", aggfunc="sum", fill_value=0
            ).reset_index()
            piv_usd.columns.name = None

            # Pivot cantidad (balance tokens)
            piv_qty = cb_f.pivot_table(
                index=["wallet", "contract_name", "network"],
                columns="base_symbol", values="balance", aggfunc="sum", fill_value=0
            ).reset_index()
            piv_qty.columns.name = None

            # Renombrar columnas
            tokens_found = [c for c in piv_usd.columns if c not in ["wallet","contract_name","network"]]
            piv_usd = piv_usd.rename(columns={t: f"{t}_usd" for t in tokens_found})
            piv_qty = piv_qty.rename(columns={t: f"{t}_qty" for t in tokens_found})

            # Merge y ordenar columnas intercaladas: TOKEN_qty | TOKEN_usd
            piv = piv_usd.merge(piv_qty[["wallet","contract_name","network"] + [f"{t}_qty" for t in tokens_found]],
                                on=["wallet","contract_name","network"], how="left")
            piv["total_usd"] = piv[[f"{t}_usd" for t in tokens_found]].sum(axis=1)
            piv = piv.sort_values("total_usd", ascending=False)

            # Orden de columnas: info + intercalado qty/usd por token + total
            interleaved = []
            for t in tokens_found:
                interleaved += [f"{t}_qty", f"{t}_usd"]
            display_cols = ["wallet", "contract_name", "network"] + interleaved + ["total_usd"]
            piv = piv[[c for c in display_cols if c in piv.columns]]

            # Formato
            fmt_cb = {"total_usd": "${:,.2f}"}
            for t in tokens_found:
                fmt_cb[f"{t}_usd"] = "${:,.2f}"
                fmt_cb[f"{t}_qty"] = "{:,.4f}"

            # Fila total
            total_cb = {c: "" for c in piv.columns}
            total_cb["contract_name"] = "TOTAL"
            for c in [f"{t}_usd" for t in tokens_found] + [f"{t}_qty" for t in tokens_found] + ["total_usd"]:
                if c in piv.columns:
                    total_cb[c] = piv[c].sum()
            piv_display = pd.concat([piv, pd.DataFrame([total_cb])], ignore_index=True)

            st.dataframe(
                piv_display.style.format(fmt_cb, na_rep=""),
                use_container_width=True, height=420,
            )
            st.caption("Fuente: Dune balances_polygon + balances_ethereum. Solo tokens con valor > $0.01. Columnas: _qty = cantidad de tokens, _usd = valor en USD.")
        else:
            st.info("Sin datos. Ejecutá el pipeline primero.")

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # ── 1. Arch Tokens ────────────────────────────────────────────────────
        st.markdown('<div class="section-title">Arch Tokens</div>', unsafe_allow_html=True)
        arch_tokens = [
            ("WEB3",  "Arch Web3",                  "0xc4ea087fc2cb3a1d9ff86c676f03abe4f3ee906f", "0x8F0d5660929cA6ac394c5c41f59497629b1dbc23", "1.95%", "🏄‍♂️ Active"),
            ("CHAIN", "Arch Blockchains",            "0x70a13201df2364b634cb5aac8d735db3a654b30c", "0x89c53B02558E4D1c24b9Bf3beD1279871187EF0B", "1.95%", "🏄‍♂️ Active"),
            ("ACAI",  "Arch Crypto AI",              "0x9f5c845a178dfcb9abe1e9d3649269826ce43901", "0xd1ce69b4bdd3dda553ea55a2a57c21c65190f3d5", "1.95%", "🏄‍♂️ Active"),
            ("ADDY",  "Arch USD Diversified Yield",  "0xab1b1680f6037006e337764547fb82d17606c187", "0xE15A66b7B8e385CAa6F69FD0d55984B96D7263CF", "0.25%", "🏄‍♂️ Active"),
            ("AEDY",  "Arch ETH Diversified Yield",  "0x027af1e12a5869ed329be4c05617ad528e997d5a", "0x103bb3EBc6F61b3DB2d6e01e54eF7D9899A2E16B", "0.49%", "🏄‍♂️ Active"),
            ("ABDY",  "Arch BTC Diversified Yield",  "0xef7b6cd33afafc36379289b7accae95116e27c88", "0xdE2925D582fc8711a0E93271c12615Bdd043Ed1C", "0.49%", "🏄‍♂️ Active"),
            ("AAGG",  "Arch Aggressive Portfolio",   "0xafb6e8331355fae99c8e8953bb4c6dc5d11e9f3c", "N/A",                                        "2%",    "🏄‍♂️ Active"),
            ("AMOD",  "Arch Moderate Portfolio",     "0xa5a979aa7f55798e99f91abe815c114a09164beb", "N/A",                                        "2%",    "🏄‍♂️ Active"),
            ("ABAL",  "Arch Balanced Portfolio",     "0xf401e2c1ce8f252947b60bfb92578f84217a1545", "N/A",                                        "2%",    "🏄‍♂️ Active"),
            ("ARWA",  "Arch Real World Assets",      "N/A",                                        "0xf436e681574220471fc72e42ae33564512dafd06", "1.95%", "🚀 Ready"),
            ("AP60",  "Arch Moderate Portfolio (SET)","0x6ca9c8914a14d63a6700556127d09e7721ff7d3b","N/A",                                        "2%",    "⚰️ Deprecated"),
            ("ABDY",  "ABDY V1",                     "0xde2925d582fc8711a0e93271c12615bdd043ed1c", "N/A",                                        "0.49%", "⚰️ Deprecated"),
        ]
        t = f"<table style='width:100%;border-collapse:collapse'><thead><tr><th {TH}>Symbol</th><th {TH}>Nombre</th><th {TH}>POL Address</th><th {TH}>ETH Address</th><th {TH}>Fee</th><th {TH}>Estado</th></tr></thead><tbody>"
        for sym, name, pol, eth, fee, state in arch_tokens:
            t += (f"<tr><td {TDM}>{sym}</td><td {TD}>{name}</td>"
                  f"<td {TDA}>{scan_link(pol,'POL')}</td><td {TDA}>{scan_link(eth,'ETH')}</td>"
                  f"<td {TD}>{fee}</td><td {TD}>{state_badge(state)}</td></tr>")
        st.markdown(t + "</tbody></table>", unsafe_allow_html=True)

        # ── 2. Archemists ─────────────────────────────────────────────────────
        st.markdown('<div class="section-title">Archemists (Vaults)</div>', unsafe_allow_html=True)
        archemists = [
            ("WEB3",  "USDC", "0.5%", "0xC68140cdf17566F8AD43db8487d6600196d79176", "🏄‍♂️ Active"),
            ("CHAIN", "USDC", "0.5%", "0xC770C918332522aC66306a83989ba1b5B807c1ae", "🏄‍♂️ Active"),
            ("ACAI",  "USDC", "0.5%", "0x2C0c8A17a58d37F0cA75cf9482307a8c6043d252", "🏄‍♂️ Active"),
            ("ABDY",  "USDC", "0.5%", "0xCFA916cCeb4a32d727210B21b7A23FBCcC5c4e7D","🏄‍♂️ Active"),
            ("ADDY",  "USDC", "0.1%", "0x774Aac3a6F0Da70D57621dE098cf4d2Ef77bf1A5", "🏄‍♂️ Active"),
            ("AEDY",  "USDC", "0.5%", "0x578934dF6e0f1d525Ee5cE3397A7c4C554945efA", "🏄‍♂️ Active"),
            ("ACAI",  "USDC", "1%",   "0x62453a04c7be8196b37f45e5c7a928a96fe0db94", "⚰️ Deprecated"),
            ("WEB3",  "USDC", "50%",  "0xD551Db49374b6C2FD8056041924026619bE6f16E", "⚰️ Deprecated"),
            ("CHAIN", "USDC", "0.1%", "0x081fb37c068ad52103cfd7f2be67a70a14ac39bb", "⚰️ Deprecated"),
            ("ADDY",  "USDC", "1%",   "0x4feefebaad8a892a29ecf885397c705fc03cb73b", "⚰️ Deprecated"),
            ("AEDY",  "USDC", "1%",   "0xe98d44437da215313fb634ed766282f770690c1c", "⚰️ Deprecated"),
        ]
        t = f"<table style='width:100%;border-collapse:collapse'><thead><tr><th {TH}>Token</th><th {TH}>Exchange</th><th {TH}>Fee</th><th {TH}>POL Address</th><th {TH}>Estado</th></tr></thead><tbody>"
        for tok, exch, fee, pol, state in archemists:
            t += (f"<tr><td {TDM}>{tok}</td><td {TD}>{exch}</td><td {TD}>{fee}</td>"
                  f"<td {TDA}>{scan_link(pol,'POL')}</td><td {TD}>{state_badge(state)}</td></tr>")
        st.markdown(t + "</tbody></table>", unsafe_allow_html=True)

        # ── 3. Contratos Operacionales ────────────────────────────────────────
        st.markdown('<div class="section-title">Contratos Operacionales</div>', unsafe_allow_html=True)
        # Columns: (Nombre, POL address, ETH address, Descripción, Estado)
        # "N/A" = no existe en esa red
        operational = [
            ("Arch Ramp",               "0x6eEabA794883F75a1e6E9a38426207e853a6Df58", "N/A",                                        "On & OffRamps",           "🏄‍♂️ Active"),
            ("Gasworks",                "0xf67df2fd4a56046eacf03e3762b2495cfdedf271", "N/A",                                        "User Operations",         "🏄‍♂️ Active"),
            ("Migration Escrow",        "0xbc13b615c6630326a15e312c345619da756226a1", "N/A",                                        "Products Migration",      "🏄‍♂️ Active"),
            ("Trade Issuer V3",         "0xdCB99117Ba207b996EE3c49eE6F8c0f1d371867A", "0x92b6a2AEE6c748AD196Fbfd449F87c9B2aA2e519", "Swaps para issuance",     "🏄‍♂️ Active"),
            ("Arch Nexus",              "0xfde21d887b245849e2509163582ce0bbc90fcc4c", "0x8648B1E944e1322eC914E6DE015Dc660F627927C", "Contract Calls",          "🏄‍♂️ Active"),
            ("Archemist God",           "0xE1E9568B9F735Cafb282BB164687d4c37587Bf90", "N/A",                                        "Archemist Factory",       "🏄‍♂️ Active"),
            ("Backoffice Login",        "0xb2709612c105b86c44Ba0150456E47ca248d7685", "N/A",                                        "Backoffice Login",        "🏄‍♂️ Active"),
            ("Structured Funds Factory","0xb3F2cC719dCadcA9133074aa37964Cb972FB3d82", "N/A",                                        "Factory structured funds","🏄‍♂️ Active"),
            ("CEX Ramp",                "0x3Ddd928a5d1be641C0bF2727a078f1342a1A6c0E", "0x3Ddd928a5d1be641C0bF2727a078f1342a1A6c0E","CEX Ramps",               "🏄‍♂️ Active"),
            ("Koywe Ramp",              "0xD461ECAC15CC2891a588cE283933065d1125Db6c", "N/A",                                        "OnRamps",                 "🏄‍♂️ Active"),
            ("Koywe OffRamp",           "0xa8b21F3cbC89E6D88f31b9486aa5A5C37560E471", "N/A",                                        "OffRamps",                "🏄‍♂️ Active"),
            ("Gasless",                 "0x9Ea1f32A606A2956345444AA7c0DCfe6CcAB30F4", "N/A",                                        "Products Exchange",       "🏄‍♂️ Active"),
            ("Faucet",                  "0x66eE243E25D67DcEc02874102F68809a597060BD", "N/A",                                        "Migrations / Ramps",      "🏄‍♂️ Active"),
            ("Referrals",               "0x217216913438Fa9E305187727963DbF595D4d796", "N/A",                                        "Referrals",               "🏄‍♂️ Active"),
            ("Fiat Ramp (DCA)",         "0x7F7214C19A2Ad6c5A7D07d2E187DE1a008a7BEa9", "N/A",                                        "DCA / OnRamps",           "🏄‍♂️ Active"),
            ("Development Ops",         "0xf33F0262dD37c9ae09393d09764aa363dcdC9627", "N/A",                                        "All Ops",                 "🏄‍♂️ Active"),
            ("Liquidity Manager",       "0x131067246BBD3c94c82e0B74c71D430e81da950b", "0x131067246BBD3c94c82e0B74c71D430e81da950b","Collect Fees",            "🏄‍♂️ Active"),
            ("Archemist Operator",      "0x5953e8E6070287C63eE95480a4768FaA5DD3F405", "N/A",                                        "Archemists",              "🏄‍♂️ Active"),
            ("Trade Issuer Operator",   "0xe560EfD37a77486aa0ecAed4203365BDe5363dbB", "N/A",                                        "Trade Issuer",            "🏄‍♂️ Active"),
            ("ALPS",                    "0x0a0044E0521ccD7cd61fE4c943E2E95b149659E9", "N/A",                                        "Liquidity Position Strat","🏄‍♂️ Active"),
            ("Arch Leverage ETH",       "0xf01c18deef438f3a5e4bb27404b4b44911625300", "N/A",                                        "Leverage ETH en AAVE",    "🏄‍♂️ Active"),
        ]
        t = f"<table style='width:100%;border-collapse:collapse'><thead><tr><th {TH}>Nombre</th><th {TH}>POL Address</th><th {TH}>ETH Address</th><th {TH}>Descripción</th><th {TH}>Estado</th></tr></thead><tbody>"
        for name, pol, eth, desc, state in operational:
            t += (f"<tr><td {TDM}>{name}</td><td {TDA}>{scan_link(pol,'POL')}</td>"
                  f"<td {TDA}>{scan_link(eth,'ETH')}</td><td {TD}>{desc}</td><td {TD}>{state_badge(state)}</td></tr>")
        st.markdown(t + "</tbody></table>", unsafe_allow_html=True)

        # ── 4. Chamber Ecosystem ──────────────────────────────────────────────
        st.markdown('<div class="section-title">Chamber Ecosystem</div>', unsafe_allow_html=True)
        chamber = [
            ("Chamber God",         "0x0C9Aa1e4B4E39DA01b7459607995368E4C38cFEF", "0x0C9Aa1e4B4E39DA01b7459607995368E4C38cFEF", "Products Factory",     "🏄‍♂️ Active"),
            ("Issuer Wizard",       "0x60F56236CD3C1Ac146BD94F2006a1335BaA4c449", "0x60F56236CD3C1Ac146BD94F2006a1335BaA4c449", "Tokens Issuance",      "🏄‍♂️ Active"),
            ("Streaming Fee Wizard","0xDD5211D669f5B1f19991819Bbd8B220DbBf8062E", "0xDD5211D669f5B1f19991819Bbd8B220DbBf8062E", "Fees Collection",      "🏄‍♂️ Active"),
            ("Rebalance Wizard",    "0x13541eA37cfB0cE3bfF8f28D468D93b348BcDdea", "0x13541eA37cfB0cE3bfF8f28D468D93b348BcDdea", "Products Rebalance",   "🏄‍♂️ Active"),
            ("Trade Issuer V3",     "0x92b6a2AEE6c748AD196Fbfd449F87c9B2aA2e519", "0xdCB99117Ba207b996EE3c49eE6F8c0f1d371867A", "Swaps para issuance",  "🏄‍♂️ Active"),
            ("Arch Nexus",          "0x8648B1E944e1322eC914E6DE015Dc660F627927C", "0xfde21d887b245849e2509163582ce0bbc90fcc4c", "Contract Calls",       "🏄‍♂️ Active"),
            ("Trade Issuer V2",     "0xbbCA2AcBd87Ce7A5e01fb56914d41F6a7e5C5A56", "N/A",                                        "Deprecated",           "⚰️ Deprecated"),
        ]
        t = f"<table style='width:100%;border-collapse:collapse'><thead><tr><th {TH}>Nombre</th><th {TH}>ETH Address</th><th {TH}>POL Address</th><th {TH}>Descripción</th><th {TH}>Estado</th></tr></thead><tbody>"
        for name, eth, pol, desc, state in chamber:
            t += (f"<tr><td {TDM}>{name}</td><td {TDA}>{scan_link(eth,'ETH')}</td>"
                  f"<td {TDA}>{scan_link(pol,'POL')}</td><td {TD}>{desc}</td><td {TD}>{state_badge(state)}</td></tr>")
        st.markdown(t + "</tbody></table>", unsafe_allow_html=True)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    data = load_data()
    ws_raw = data.get("wallet_summary", pd.DataFrame())
    filters = render_sidebar(ws_raw)
    ws = apply_filters(ws_raw, filters)

    # Header
    h1, h2 = st.columns([4, 1])
    with h1:
        st.markdown("## ⬡ Arch Finance — Monitor de Cierre")
    with h2:
        if not ws_raw.empty:
            n = len(ws); tot = len(ws_raw)
            st.markdown(f"<div style='text-align:right;padding-top:14px;font-size:12px;color:#4B5675'>{n:,} wallets mostradas de {tot:,}</div>", unsafe_allow_html=True)

    if not check_data():
        st.warning("Sin datos. Ejecutá `python pipeline.py --csv-dir ./data/raw/` y recargá.")
        return
    if ws.empty:
        st.info("Sin datos con los filtros actuales.")
        return

    render_overview(ws)
    render_segments(ws)
    of_f = apply_filters(data.get("outflows", pd.DataFrame()), {"segment": filters["segment"]})
    render_charts(ws, of_f)
    bal_f = apply_filters(data.get("balances", pd.DataFrame()), {"segment": filters["segment"]})
    render_tokens(bal_f, of_f)
    render_tables(ws, data.get("balances", pd.DataFrame()), data.get("outflows", pd.DataFrame()), filters)


if __name__ == "__main__":
    main()
