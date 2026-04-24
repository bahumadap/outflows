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
    for name in ["balances", "outflows", "wallet_summary", "pools", "supply", "unknown_wallets", "reconciliation"]:
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
        seg = st.multiselect("Segmento", ["preferente", "retail"], default=["preferente", "retail"], label_visibility="collapsed")
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
    n_active   = (ws["total_balance_usd"] > 1).sum()
    n_done     = (ws["status"] == "Retirado completamente").sum()
    n_partial  = (ws["status"] == "Retiro parcial").sum()
    n_inactive = (ws["status"] == "Sin movimiento").sum()

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
        p_done = pct(n_done, n_total)
        st.markdown(f"""
        <div class="card">
            <div class="t-label">Wallets · Estado</div>
            <div class="t-value-lg">{n_active}<span style="font-size:16px;color:#4B5675"> / {n_total}</span></div>
            {track(p_done, "track-fill-red")}
            <div class="t-sub">
                <span class="dot-red">●</span> {n_done} retirados ({p_done:.0f}%)&ensp;
                <span class="dot-yellow">●</span> {n_partial} parcial&ensp;
                <span class="dot-dim">●</span> {n_inactive} sin mov.
            </div>
        </div>""", unsafe_allow_html=True)


# ─── SEGMENTS ─────────────────────────────────────────────────────────────────

def render_segments(ws: pd.DataFrame):
    st.markdown('<div class="section-title">Desglose por Segmento</div>', unsafe_allow_html=True)

    pref   = ws[ws["segment"] == "preferente"]
    retail = ws[ws["segment"] == "retail"]

    def seg_card(df, label, track_cls, c_accent):
        rem = df["total_balance_usd"].sum()
        out = df["total_outflow_usd"].sum()
        aum = rem + out
        p_o = pct(out, aum)
        p_r = pct(rem, aum)
        n   = len(df)
        n_done    = int((df["status"] == "Retirado completamente").sum())
        n_partial = int((df["status"] == "Retiro parcial").sum())
        n_sinmov  = int((df["status"] == "Sin movimiento").sum())

        TD = "style='padding:5px 0;border-bottom:1px solid #111827;'"
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

    c1, c2 = st.columns(2)
    with c1: seg_card(pref,   "🏢  PREFERENTE", "track-fill-blue",   C_BLUE)
    with c2: seg_card(retail, "👤  RETAIL",     "track-fill-orange", C_ORANGE)


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

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "Wallets", "Balances", "Retiros", "Análisis", "Pools", unk_label, "⚖️ Reconciliación",
    ])

    fmt_style = {"total_balance_usd": "${:,.2f}", "total_outflow_usd": "${:,.2f}", "pct_withdrawn": "{:.1f}%"}

    with tab1:
        if not ws.empty:
            cols = ["customer_name", "email", "segment", "network", "status",
                    "total_balance_usd", "total_outflow_usd", "pct_withdrawn",
                    "num_outflow_events", "last_outflow_date", "tokens_held", "wallet_address"]
            av = [c for c in cols if c in ws.columns]
            st.dataframe(ws[av].style.format(fmt_style), use_container_width=True, height=540)
            st.download_button("Descargar CSV", ws[av].to_csv(index=False), "wallets.csv", "text/csv")

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
        rec = data.get("reconciliation", pd.DataFrame())

        if rec.empty:
            st.info("Sin datos de reconciliación. Ejecutá el pipeline primero.")
        else:
            # ── Totales globales ──────────────────────────────────────────────
            total_cap    = rec["market_cap_usd"].sum()
            total_all    = rec["all_holders_usd"].sum() if "all_holders_usd" in rec.columns else 0
            total_client = rec["client_usd"].sum() if "client_usd" in rec.columns else 0
            total_arch   = rec["arch_other_usd"].sum() if "arch_other_usd" in rec.columns else 0
            n_warn       = rec["alerta"].str.startswith("⚠️").sum()

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Market Cap total", f"${total_cap:,.0f}",
                      help="supply × precio — valor de todos los tokens en circulación")
            m2.metric("AUM clientes registrados", f"${total_client:,.0f}",
                      help="Lo que tienen nuestros clientes monitoreados")
            m3.metric("Arch / contratos / otros", f"${total_arch:,.0f}",
                      help="Tokens en contratos Arch, pools, wallets no registradas")
            m4.metric("Alertas", int(n_warn),
                      delta=None if n_warn == 0 else "⚠️ revisar")

            if n_warn > 0:
                st.error(
                    f"**{n_warn} token{'s' if n_warn > 1 else ''} con inconsistencia detectada.** "
                    "Puede indicar error en el supply de Dune o en el precio NAV."
                )
            else:
                st.success("Cruce OK — Market Cap ≈ suma on-chain para todos los tokens.")

            # Barra visual: clientes vs arch/otros vs "no contabilizado"
            pct_client = total_client / max(total_cap, 1) * 100
            pct_arch   = total_arch   / max(total_cap, 1) * 100
            st.markdown(
                f"<div style='margin:16px 0 4px 0'>"
                f"<div style='display:flex;gap:0;height:8px;border-radius:4px;overflow:hidden;background:#1C2333'>"
                f"<div style='width:{pct_client:.1f}%;background:#3DD68C'></div>"
                f"<div style='width:{pct_arch:.1f}%;background:#60A5FA'></div>"
                f"</div>"
                f"<div style='display:flex;gap:16px;margin-top:6px;font-size:11px;color:#4B5675'>"
                f"<span><span style='color:#3DD68C'>●</span> Clientes {pct_client:.1f}%</span>"
                f"<span><span style='color:#60A5FA'>●</span> Arch/otros {pct_arch:.1f}%</span>"
                f"<span><span style='color:#1C2333'>●</span> Delta supply {100-pct_client-pct_arch:.1f}%</span>"
                f"</div></div>",
                unsafe_allow_html=True,
            )

            st.markdown("---")

            # ── Tabla por token ───────────────────────────────────────────────
            TH  = "style='padding:8px 12px;font-size:10px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#4B5675;border-bottom:1px solid #1C2333;text-align:right'"
            THL = "style='padding:8px 12px;font-size:10px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#4B5675;border-bottom:1px solid #1C2333'"
            TD  = "style='padding:9px 12px;border-bottom:1px solid #111827;font-size:12px'"
            TDR = "style='padding:9px 12px;border-bottom:1px solid #111827;font-size:12px;text-align:right;font-variant-numeric:tabular-nums'"

            table_html = (
                "<table style='width:100%;border-collapse:collapse'>"
                "<thead><tr>"
                f"<th {THL}>Token</th>"
                f"<th {TH}>Supply</th>"
                f"<th {TH}>Precio</th>"
                f"<th {TH}>Market Cap</th>"
                f"<th {TH}>Clientes (USD)</th>"
                f"<th {TH}>Arch / otros</th>"
                f"<th {TH}>Delta supply</th>"
                f"<th {TH}>% clientes</th>"
                f"<th {TH}>Estado</th>"
                "</tr></thead><tbody>"
            )

            for _, row in rec.iterrows():
                alerta = row["alerta"]
                alerta_color = "#F87171" if alerta.startswith("⚠️") else ("#3DD68C" if alerta == "✓ OK" else "#4B5675")

                pct_c    = row.get("pct_client", 0)
                bar_w    = min(pct_c, 100)
                bar_col  = "#3DD68C" if pct_c <= 90 else "#FBBF24" if pct_c <= 100 else "#F87171"
                pct_bar  = (
                    f"<div style='display:flex;align-items:center;gap:5px;justify-content:flex-end'>"
                    f"<div style='width:50px;height:4px;background:#1C2333;border-radius:2px;overflow:hidden'>"
                    f"<div style='width:{bar_w:.0f}%;height:100%;background:{bar_col}'></div></div>"
                    f"<span style='color:{bar_col}'>{pct_c:.1f}%</span></div>"
                )

                delta     = row.get("delta_usd", 0)
                delta_pct = row.get("delta_pct", 0)
                delta_col = "#F87171" if delta_pct > 10 else "#4B5675"
                delta_fmt = f"<span style='color:{delta_col}'>${delta:+,.0f} ({delta_pct:.1f}%)</span>"

                cap_fmt  = f"${row['market_cap_usd']:,.0f}" if row['market_cap_usd'] > 0 else "—"
                all_fmt  = f"${row.get('all_holders_usd', 0):,.0f}"
                cli_fmt  = f"${row.get('client_usd', 0):,.0f}"
                arch_fmt = f"${row.get('arch_other_usd', 0):,.0f}"
                sup_fmt  = f"{row['supply_total']:,.1f}" if row['supply_total'] > 0 else "—"
                pr_fmt   = f"${row['precio_usd']:,.4f}" if row['precio_usd'] > 0 else "—"

                table_html += (
                    f"<tr>"
                    f"<td {TD}><span style='font-family:monospace;font-weight:700;color:#CDD5E0'>{row['token']}</span></td>"
                    f"<td {TDR} style='color:#6B7A99'>{sup_fmt}</td>"
                    f"<td {TDR} style='color:#9CA3AF'>{pr_fmt}</td>"
                    f"<td {TDR} style='color:#9CA3AF'>{cap_fmt}</td>"
                    f"<td {TDR} style='color:#3DD68C'>{cli_fmt}</td>"
                    f"<td {TDR} style='color:#60A5FA'>{arch_fmt}</td>"
                    f"<td {TDR}>{delta_fmt}</td>"
                    f"<td {TDR}>{pct_bar}</td>"
                    f"<td style='padding:9px 12px;border-bottom:1px solid #111827;font-size:12px;text-align:right;font-weight:600;color:{alerta_color}'>{alerta}</td>"
                    f"</tr>"
                )

            table_html += "</tbody></table>"
            st.markdown(table_html, unsafe_allow_html=True)

            st.markdown(
                "<div style='font-size:11px;color:#2D3650;margin-top:16px;padding:10px 14px;border:1px solid #1C2333;border-radius:6px;line-height:1.7'>"
                "ℹ️ <b>Metodología:</b> "
                "<b>Market Cap</b> = supply total × precio actual (lo que valen todos los tokens en circulación). "
                "<b>Clientes</b> = wallets registradas en los CSVs. "
                "<b>Arch / otros</b> = contratos Arch, pools de liquidez, wallets no registradas. "
                "<b>Delta supply</b> = Market Cap − suma on-chain (debería ser ≈ $0). "
                "A medida que los clientes rediman, el supply baja y el Market Cap tiende a $0."
                "</div>",
                unsafe_allow_html=True,
            )

            st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
            st.download_button(
                "Descargar reconciliación CSV",
                rec.to_csv(index=False),
                "reconciliacion.csv",
                "text/csv",
            )


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
