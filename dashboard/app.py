"""
GitHub Archive Lakehouse Dashboard
Run: .venv/bin/streamlit run dashboard/app.py
"""

import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog

load_dotenv()

st.set_page_config(
    page_title="GH Archive Lakehouse",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

MANIFEST_PATH = Path("data/processed/manifest.json")
CATALOG_URI   = "sqlite:///data/processed/iceberg_catalog.db"

# ── Custom CSS ────────────────────────────────────────────────────────────────

st.markdown("""
<style>
/* ── Global font & background ── */
html, body, [class*="css"] {
    font-family: 'Inter', 'Segoe UI', sans-serif;
}

/* ── Metric cards ── */
[data-testid="metric-container"] {
    background: linear-gradient(135deg, #1e1e2e 0%, #2a2a3e 100%);
    border: 1px solid #3a3a5c;
    border-radius: 12px;
    padding: 16px 20px;
    box-shadow: 0 4px 15px rgba(0,0,0,0.3), 0 1px 4px rgba(99,102,241,0.15);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}
[data-testid="metric-container"]:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 25px rgba(0,0,0,0.4), 0 2px 8px rgba(99,102,241,0.3);
}
[data-testid="stMetricLabel"] { color: #a0a0c0 !important; font-size: 0.78rem; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; }
[data-testid="stMetricValue"] { color: #e0e0ff !important; font-size: 1.8rem; font-weight: 700; }

/* ── Buttons ── */
div.stButton > button {
    background: linear-gradient(135deg, #6366f1, #8b5cf6);
    color: white;
    border: none;
    border-radius: 8px;
    padding: 0.45rem 1.2rem;
    font-weight: 600;
    font-size: 0.85rem;
    letter-spacing: 0.03em;
    cursor: pointer;
    box-shadow: 0 3px 10px rgba(99,102,241,0.4);
    transition: all 0.2s ease;
}
div.stButton > button:hover {
    background: linear-gradient(135deg, #4f46e5, #7c3aed);
    box-shadow: 0 5px 18px rgba(99,102,241,0.6);
    transform: translateY(-1px);
}
div.stButton > button:active {
    transform: translateY(0px);
    box-shadow: 0 2px 8px rgba(99,102,241,0.4);
}

/* ── Section cards ── */
.section-card {
    background: linear-gradient(135deg, #16162a 0%, #1e1e35 100%);
    border: 1px solid #2e2e50;
    border-radius: 14px;
    padding: 20px 24px;
    margin-bottom: 20px;
    box-shadow: 0 4px 20px rgba(0,0,0,0.25);
}

/* ── Header banner ── */
.hero-banner {
    background: linear-gradient(135deg, #0f0f1a 0%, #1a1a3e 50%, #0d1117 100%);
    border: 1px solid #30305a;
    border-radius: 16px;
    padding: 28px 36px;
    margin-bottom: 28px;
    box-shadow: 0 6px 30px rgba(0,0,0,0.5), inset 0 1px 0 rgba(255,255,255,0.05);
}
.hero-title {
    font-size: 2rem;
    font-weight: 800;
    background: linear-gradient(90deg, #818cf8, #c084fc, #38bdf8);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin: 0 0 6px 0;
}
.hero-sub {
    color: #6b7280;
    font-size: 0.9rem;
    margin: 0;
}
.hero-badge {
    display: inline-block;
    background: rgba(99,102,241,0.15);
    border: 1px solid rgba(99,102,241,0.3);
    color: #818cf8;
    border-radius: 20px;
    padding: 3px 12px;
    font-size: 0.75rem;
    font-weight: 600;
    margin-right: 6px;
    margin-top: 10px;
}

/* ── Section headers ── */
.section-title {
    font-size: 1.1rem;
    font-weight: 700;
    color: #c4b5fd;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    margin-bottom: 4px;
    padding-bottom: 8px;
    border-bottom: 1px solid #2e2e50;
}

/* ── Status pill ── */
.pill-green  { display:inline-block; background:rgba(16,185,129,0.15); border:1px solid rgba(16,185,129,0.4); color:#34d399; border-radius:20px; padding:2px 10px; font-size:0.75rem; font-weight:600; }
.pill-yellow { display:inline-block; background:rgba(245,158,11,0.15); border:1px solid rgba(245,158,11,0.4); color:#fbbf24; border-radius:20px; padding:2px 10px; font-size:0.75rem; font-weight:600; }
.pill-red    { display:inline-block; background:rgba(239,68,68,0.15);  border:1px solid rgba(239,68,68,0.4);  color:#f87171; border-radius:20px; padding:2px 10px; font-size:0.75rem; font-weight:600; }

/* ── Footer ── */
.footer {
    text-align: center;
    padding: 20px;
    color: #4b5563;
    font-size: 0.8rem;
    border-top: 1px solid #1f1f35;
    margin-top: 30px;
}
.footer span { color: #818cf8; font-weight: 700; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0d0d1a 0%, #111127 100%);
    border-right: 1px solid #1e1e35;
}

/* ── Selectbox ── */
[data-testid="stSelectbox"] label { color: #a0a0c0 !important; font-size: 0.8rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; }

/* ── Progress bar ── */
[data-testid="stProgressBar"] > div > div { background: linear-gradient(90deg, #6366f1, #8b5cf6); }
</style>
""", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────────────────────

@st.cache_resource
def get_catalog():
    return SqlCatalog("local", **{
        "uri": CATALOG_URI,
        "warehouse": f"s3://{os.getenv('S3_BUCKET', 'lakehouse')}",
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        "s3.access-key-id": os.getenv("S3_ACCESS_KEY", "minio"),
        "s3.secret-access-key": os.getenv("S3_SECRET_KEY", "minio123"),
        "s3.path-style-access": "true",
    })


@st.cache_data(ttl=300, show_spinner="Scanning Iceberg tables...")
def load_all():
    catalog = get_catalog()

    def safe_load(name):
        try:
            return catalog.load_table(name).scan().to_pandas()
        except Exception:
            return pd.DataFrame()

    return (
        safe_load("bronze.github_events"),
        safe_load("silver.github_events"),
        safe_load("gold.repo_daily_activity"),
        safe_load("gold.actor_daily_activity"),
        safe_load("silver.quarantine"),
    )


def load_manifest():
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style='text-align:center; padding: 16px 0 8px 0;'>
        <div style='font-size:2.2rem;'>🧊</div>
        <div style='font-weight:800; color:#818cf8; font-size:1rem; margin-top:4px;'>GH Archive</div>
        <div style='color:#4b5563; font-size:0.75rem;'>Data Lakehouse</div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    page = st.radio(
        "Navigate",
        ["🏠  Overview", "📊  Analytics", "🔍  Explorer", "🚨  Quarantine"],
        label_visibility="collapsed",
    )

    st.divider()

    st.markdown("<div style='color:#4b5563; font-size:0.75rem; font-weight:600; text-transform:uppercase; letter-spacing:0.05em;'>Stack</div>", unsafe_allow_html=True)
    for badge, color in [("Apache Iceberg", "#38bdf8"), ("MinIO", "#f59e0b"),
                         ("DuckDB", "#fbbf24"), ("PyArrow", "#34d399"), ("Streamlit", "#f472b6")]:
        st.markdown(f"<span style='display:inline-block; background:rgba(30,30,60,0.8); border:1px solid #2e2e50; border-radius:6px; padding:3px 10px; font-size:0.72rem; color:{color}; margin:2px 0;'>{badge}</span>", unsafe_allow_html=True)

    st.divider()

    if st.button("🔄  Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("""
    <div style='position:absolute; bottom:20px; left:0; right:0; text-align:center; color:#374151; font-size:0.72rem;'>
        Built by <span style='color:#818cf8; font-weight:700;'>Amitabh</span>
    </div>
    """, unsafe_allow_html=True)


# ── Load data ─────────────────────────────────────────────────────────────────

with st.spinner("Loading Iceberg tables from MinIO..."):
    bronze, silver, repo_gold, actor_gold, quarantine = load_all()

manifest   = load_manifest()
done_hours = sum(1 for v in manifest.values() if v.get("status") == "done")

# ── Hero banner ───────────────────────────────────────────────────────────────

st.markdown("""
<div class="hero-banner">
    <div class="hero-title">🧊 GitHub Archive Lakehouse</div>
    <p class="hero-sub">Real-time pipeline · Apache Iceberg on MinIO · Medallion Architecture</p>
    <span class="hero-badge">Bronze</span>
    <span class="hero-badge">Silver</span>
    <span class="hero-badge">Gold</span>
    <span class="hero-badge">2024-01-01</span>
    <span class="hero-badge">3.8M events</span>
</div>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════

if "Overview" in page:

    # ── Metric cards ──────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("⏱ Hours Ingested",  f"{done_hours} / 24",
              delta="Complete" if done_hours == 24 else f"{24 - done_hours} remaining")
    c2.metric("🥉 Bronze Rows",    f"{len(bronze):,}"     if not bronze.empty     else "—")
    c3.metric("🥈 Silver Rows",    f"{len(silver):,}"     if not silver.empty     else "—")
    c4.metric("🥇 Gold Repo-Days", f"{len(repo_gold):,}"  if not repo_gold.empty  else "—")
    c5.metric("🚨 Quarantined",    f"{len(quarantine):,}" if not quarantine.empty else "0",
              delta="Clean ✓" if quarantine.empty else "Needs review")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Pipeline progress ──────────────────────────────────────────────────────
    st.markdown('<div class="section-title">Pipeline Progress</div>', unsafe_allow_html=True)

    if manifest:
        last_ingested = max(
            (v["ingested_at"] for v in manifest.values() if v.get("status") == "done"),
            default="—"
        )
        st.progress(done_hours / 24,
                    text=f"{'✅ All 24 hours complete' if done_hours == 24 else f'{done_hours}/24 hours ingested'}  ·  Last run: {last_ingested[:19].replace('T', ' ')} UTC")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Manifest file-by-file status ──────────────────────────────────────────
    st.markdown('<div class="section-title">Hourly File Status</div>', unsafe_allow_html=True)

    if manifest:
        rows = []
        for hour in range(24):
            fname = f"2024-01-01-{hour}.json.gz"
            entry = manifest.get(fname, {})
            status = entry.get("status", "pending")
            rows.append({
                "Hour": f"{hour:02d}:00",
                "File": fname,
                "Status": "✅ Done" if status == "done" else "⏳ Pending",
                "Bronze Rows": f"{entry.get('bronze_rows', 0):,}" if status == "done" else "—",
                "Silver Rows": f"{entry.get('silver_rows', 0):,}" if status == "done" else "—",
                "Quarantined": str(entry.get("quarantine_rows", 0)) if status == "done" else "—",
                "Ingested At": entry.get("ingested_at", "—")[:19].replace("T", " ") if status == "done" else "—",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=450, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════

elif "Analytics" in page:

    if silver.empty:
        st.warning("No Silver data found. Run the ingestion pipeline first.")
    else:
        chart_bg    = "rgba(0,0,0,0)"
        paper_bg    = "rgba(0,0,0,0)"
        font_color  = "#9ca3af"
        grid_color  = "#1f1f35"

        def style_fig(fig, height=420):
            fig.update_layout(
                plot_bgcolor=chart_bg, paper_bgcolor=paper_bg,
                font_color=font_color, height=height,
                margin=dict(l=10, r=10, t=30, b=10),
                xaxis=dict(gridcolor=grid_color, linecolor=grid_color),
                yaxis=dict(gridcolor=grid_color, linecolor=grid_color),
            )
            return fig

        # ── Row 1: event type + hourly ─────────────────────────────────────────
        st.markdown('<div class="section-title">Silver Layer — Event Breakdown</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)

        with col1:
            event_counts = (
                silver["type"].value_counts().reset_index()
                .rename(columns={"type": "Event Type", "count": "Count"})
            )
            fig = px.bar(event_counts, x="Count", y="Event Type", orientation="h",
                         color="Count", color_continuous_scale="Blues",
                         title="Event Types")
            fig.update_layout(yaxis={"categoryorder": "total ascending"},
                               coloraxis_showscale=False)
            st.plotly_chart(style_fig(fig), use_container_width=True)

        with col2:
            hourly = (
                silver.assign(hour=pd.to_datetime(silver["created_at"]).dt.hour)
                ["hour"].value_counts().sort_index().reset_index()
                .rename(columns={"hour": "Hour", "count": "Events"})
            )
            fig2 = px.area(hourly, x="Hour", y="Events",
                           color_discrete_sequence=["#6366f1"],
                           title="Events Per Hour (Traffic Shape)")
            fig2.update_traces(fill="tozeroy", fillcolor="rgba(99,102,241,0.15)")
            st.plotly_chart(style_fig(fig2), use_container_width=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── Row 2: Gold insights ───────────────────────────────────────────────
        st.markdown('<div class="section-title">Gold Layer — Top Repos & Actors</div>', unsafe_allow_html=True)

        top_n = st.slider("Show top N", min_value=5, max_value=30, value=15, step=5)

        col3, col4 = st.columns(2)

        with col3:
            if not repo_gold.empty:
                top_repos = repo_gold.nlargest(top_n, "total_events")[
                    ["repo_name", "total_events", "push_count", "stars", "forks"]
                ].reset_index(drop=True)
                fig3 = px.bar(
                    top_repos, x="total_events", y="repo_name", orientation="h",
                    color="push_count", color_continuous_scale="Oranges",
                    labels={"total_events": "Total Events", "repo_name": "Repo", "push_count": "Pushes"},
                    hover_data=["stars", "forks"],
                    title=f"Top {top_n} Repos by Events",
                )
                fig3.update_layout(yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)
                st.plotly_chart(style_fig(fig3, height=500), use_container_width=True)

        with col4:
            if not actor_gold.empty:
                top_actors = actor_gold.nlargest(top_n, "total_commits")[
                    ["actor_login", "total_commits", "push_count", "repos_touched"]
                ].reset_index(drop=True)
                fig4 = px.bar(
                    top_actors, x="total_commits", y="actor_login", orientation="h",
                    color="repos_touched", color_continuous_scale="Purples",
                    labels={"total_commits": "Commits", "actor_login": "Actor", "repos_touched": "Repos"},
                    title=f"Top {top_n} Actors by Commits",
                )
                fig4.update_layout(yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)
                st.plotly_chart(style_fig(fig4, height=500), use_container_width=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── Row 3: PR actions pie ──────────────────────────────────────────────
        st.markdown('<div class="section-title">Pull Request Actions</div>', unsafe_allow_html=True)
        col5, col6 = st.columns([1, 2])

        with col5:
            pr_df = silver[silver["type"] == "PullRequestEvent"]
            if not pr_df.empty and "action" in pr_df.columns:
                pr_actions = pr_df["action"].value_counts().reset_index()
                pr_actions.columns = ["Action", "Count"]
                fig5 = px.pie(pr_actions, names="Action", values="Count",
                              color_discrete_sequence=px.colors.sequential.Purples_r,
                              title="PR Actions")
                fig5.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(style_fig(fig5, height=320), use_container_width=True)

        with col6:
            if not repo_gold.empty:
                scatter = repo_gold[repo_gold["stars"] > 0].nlargest(200, "total_events")
                fig6 = px.scatter(
                    scatter, x="push_count", y="stars",
                    size="total_events", color="forks",
                    hover_name="repo_name",
                    color_continuous_scale="Viridis",
                    labels={"push_count": "Push Events", "stars": "Stars", "forks": "Forks"},
                    title="Repos: Pushes vs Stars (size = total events)",
                )
                st.plotly_chart(style_fig(fig6, height=320), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: EXPLORER
# ══════════════════════════════════════════════════════════════════════════════

elif "Explorer" in page:

    st.markdown('<div class="section-title">Silver Table Explorer</div>', unsafe_allow_html=True)

    if silver.empty:
        st.warning("No Silver data. Run the ingestion pipeline first.")
    else:
        col_f1, col_f2, col_f3 = st.columns([2, 2, 1])

        with col_f1:
            event_types = ["All"] + sorted(silver["type"].dropna().unique().tolist())
            selected_type = st.selectbox("Event Type", event_types)

        with col_f2:
            actor_search = st.text_input("Actor login contains", placeholder="e.g. github-actions")

        with col_f3:
            row_limit = st.selectbox("Rows", [100, 250, 500, 1000], index=1)

        filtered = silver.copy()
        if selected_type != "All":
            filtered = filtered[filtered["type"] == selected_type]
        if actor_search:
            filtered = filtered[filtered["actor_login"].str.contains(actor_search, case=False, na=False)]

        st.markdown(f"<span class='pill-green'>{len(filtered):,} rows match</span>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

        display_cols = ["id", "type", "actor_login", "repo_name", "created_at",
                        "action", "ref", "commit_count", "pr_number", "issue_number"]
        display_cols = [c for c in display_cols if c in filtered.columns]

        st.dataframe(filtered[display_cols].head(row_limit),
                     use_container_width=True, height=500, hide_index=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="section-title">Bronze Table Preview</div>', unsafe_allow_html=True)

        if not bronze.empty:
            st.dataframe(bronze.head(100), use_container_width=True, height=300, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: QUARANTINE
# ══════════════════════════════════════════════════════════════════════════════

elif "Quarantine" in page:

    st.markdown('<div class="section-title">Quarantine Table — Bad Records</div>', unsafe_allow_html=True)

    if quarantine.empty:
        st.markdown("""
        <div style='background:rgba(16,185,129,0.1); border:1px solid rgba(16,185,129,0.3);
                    border-radius:12px; padding:24px; text-align:center; margin-top:16px;'>
            <div style='font-size:2rem;'>✅</div>
            <div style='color:#34d399; font-size:1.1rem; font-weight:700; margin-top:8px;'>All records passed validation</div>
            <div style='color:#6b7280; font-size:0.85rem; margin-top:4px;'>No quarantined records found in silver.quarantine</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"<span class='pill-red'>{len(quarantine):,} quarantined records</span>",
                    unsafe_allow_html=True)

        if "error_reason" in quarantine.columns:
            col_q1, col_q2 = st.columns([1, 2])
            with col_q1:
                reason_counts = quarantine["error_reason"].value_counts().reset_index()
                reason_counts.columns = ["Reason", "Count"]
                fig_q = px.pie(reason_counts, names="Reason", values="Count",
                               color_discrete_sequence=["#f87171", "#fbbf24", "#fb923c"],
                               title="Quarantine Reasons")
                fig_q.update_traces(textposition="inside", textinfo="percent+label")
                fig_q.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                                    font_color="#9ca3af", height=300)
                st.plotly_chart(fig_q, use_container_width=True)

        st.dataframe(quarantine, use_container_width=True, height=400, hide_index=True)


# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown("""
<div class="footer">
    🧊 GitHub Archive Lakehouse &nbsp;·&nbsp;
    Built with ❤️ by <span>Amitabh</span> &nbsp;·&nbsp;
    Apache Iceberg · MinIO · DuckDB · Streamlit &nbsp;·&nbsp;
    Data auto-refreshes every 5 min
</div>
""", unsafe_allow_html=True)
