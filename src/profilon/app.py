# src/profilon/app.py

import base64
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components
from utils.theme import inject_theme  # keep short import path

# -------------------------
# Page + theme
# -------------------------
st.set_page_config(page_title="profilon", layout="wide")
inject_theme()  # CSS variables + base styles

# -------------------------
# Asset loader (base64)
# -------------------------
def _load_logo_b64() -> str | None:
    here = Path(__file__).resolve()
    candidates = [
        here.parent / "assets" / "cla_logo_white.png",     # src/profilon/assets/...
        here.parents[1] / "assets" / "cla_logo_white.png", # repo-root/assets/...
        here.parents[1] / "Assets" / "cla_logo_white.png", # legacy casing
    ]
    for p in candidates:
        try:
            with open(p, "rb") as fh:
                return base64.b64encode(fh.read()).decode("utf-8")
        except Exception:
            continue
    return None

_logo_b64 = _load_logo_b64()

# -------------------------
# Page-local CSS
# -------------------------
st.markdown(
    """
    <style>
      /* Give the app some breathing room so header/logo never clip */
      .block-container { padding-top: 24px; }

      /* Full-bleed container: stretches to viewport edges */
      .pf-bleed { width: 100vw; margin-left: calc(50% - 50vw); }

      /* Gradient header (dark -> light so white title stays readable) */
      .pf-banner {
        display: flex; align-items: center; justify-content: space-between;
        gap: 16px;
        padding: 18px 22px;
        border-radius: 0;               /* full-bleed, no outer radius */
        background: linear-gradient(135deg,
                    var(--cla-riptide-shade-medium, #39A5A7) 0%,
                    var(--cla-riptide, #7DD2D3) 55%,
                    var(--cla-riptide-tint-light, #C2EAEA) 100%);
        box-shadow:
          0 8px 22px rgba(0,0,0,.28),
          inset 0 1px 0 rgba(255,255,255,.10);
        border-bottom: 1px solid rgba(255,255,255,.10);
        position: relative;
        margin-top: 8px;               /* nudge down slightly to avoid any clipping */
      }
      .pf-banner::before {
        content: "";
        position: absolute; inset: 0 0 auto 0; height: 36%;
        background: linear-gradient(to bottom, rgba(255,255,255,.14), rgba(255,255,255,0));
        pointer-events: none;
      }

      .pf-banner__left { display: flex; flex-direction: column; }
      .pf-banner__title {
        margin: 0;
        color: #FFFFFF;                 /* white title */
        text-shadow: 0 1px 0 rgba(0,0,0,.25);
        font-size: 40px;
        letter-spacing: -0.3px;
        line-height: 1.12;
      }
      .pf-banner__sub {
        margin-top: 4px;
        color: rgba(0,0,0,.65);         /* readable on lighter mid/right side */
        font-weight: 800;
        text-shadow: 0 1px 0 rgba(255,255,255,.15);
      }

      /* Logo inside header (right), smaller + floating (no bevel/border/shadow) */
      .pf-logo {
        width: 70px;
        height: auto;
        display: block;
        margin: 0;
        background: transparent !important;
        border: none !important;
        border-radius: 0 !important;
        box-shadow: none !important;
        filter: none !important;
      }

      .pf-hr,
      .cla-hr { height: 1px; background: rgba(255,255,255,.08); margin: 14px 0 16px 0; }

      /* Focus/open highlight for inputs (incl. dropdowns) */
      div[data-baseweb="select"][aria-expanded="true"],
      div[data-baseweb="select"]:focus-within {
        box-shadow:
          0 0 0 2px var(--cla-riptide, #7DD2D3),
          0 0 18px rgba(125,210,211,.35) !important;
        border-radius: 10px;
        transition: box-shadow .12s ease-in-out;
      }
      .stTextInput:focus-within,
      .stNumberInput:focus-within,
      .stSlider:focus-within,
      .stMultiSelect:focus-within {
        box-shadow:
          0 0 0 2px var(--cla-riptide, #7DD2D3),
          0 0 18px rgba(125,210,211,.35) !important;
        border-radius: 10px;
        transition: box-shadow .12s ease-in-out;
      }

      /* Sidebar clock pinned at the very top-right */
      section[data-testid="stSidebar"] { position: relative; }
      #lmg-clock-wrap {
        position: absolute; top: 6px; right: 10px; z-index: 999;
        text-align: right;
        width: calc(100% - 20px);
        pointer-events: none; /* don't block nav clicks */
      }
      #lmg-clock {
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        font-size: 28px; letter-spacing: 2px;
        color: var(--cla-riptide, #7DD2D3);
        text-shadow: 0 0 8px rgba(125,210,211,.6), 0 0 22px rgba(73,191,193,.45);
      }

      /* Footer: centered LMGDATA (smaller), no bike */
      .pf-footer {
        display: block;
        text-align: center;
        padding: 10px 12px 18px 12px;
        border-top: 1px solid rgba(255,255,255,.08);
        margin-top: 18px;
      }
      .lmg-mark {
        margin-top: 6px;
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        font-size: 20px;
        letter-spacing: 3px;
        color: #0DF;
        text-shadow:
          0 0 6px rgba(0,221,255,.75),
          0 0 16px rgba(0,221,255,.45);
        display: inline-block;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# -------------------------
# Sidebar clock (absolute top-right)
# -------------------------
with st.sidebar:
    components.html(
        """
        <div id="lmg-clock-wrap">
          <div id="lmg-clock">--</div>
        </div>
        <script>
          function pad(n){return n<10?'0'+n:n}
          function tick(){
            const d = new Date();
            const s = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
            const el = document.getElementById('lmg-clock');
            if (el) el.textContent = s;
          }
          setInterval(tick, 1000); tick();
        </script>
        """,
        height=0,  # invisible container; clock is absolutely positioned
    )

# -------------------------
# Full-bleed header with logo inside
# -------------------------
logo_html = f'<img class="pf-logo" alt="CLA logo" src="data:image/png;base64,{_logo_b64}" />' if _logo_b64 else ""
st.markdown(
    f"""
    <div class="pf-bleed">
      <div class="pf-banner">
        <div class="pf-banner__left">
          <h1 class="pf-banner__title">profilon</h1>
          <div class="pf-banner__sub">turn on insight, <span class="accent">turn on trust</span></div>
        </div>
        {logo_html}
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# -------------------------
# Collapsible sections (kept, no emoji)
# -------------------------
with st.expander("Getting started", expanded=False):
    st.markdown(
        """
- **Configure & Run** — choose *pipeline / catalog / schema / table*, set profile options, and save/trigger the job.
- **Create Custom Checks** — compose rule objects with function + arguments; export to YAML.
- **View Checks** — run a lightweight preview job and fetch results without a SQL Warehouse.
        """
    )

with st.expander("General notes on DQX", expanded=False):
    st.markdown(
        """
- Use dataset-level checks for cross-row logic (aggregations, foreign keys, dataset comparisons).
- Use row-level checks for per-row constraints (nulls, ranges, regex, timestamp windows).
- Prefer **foreign_key** over **is_in_list** for large allowed-value sets.
- Document each rule with a **name** and optional **filter** for scoped enforcement.
        """
    )

with st.expander("Seven pillars of data quality (quick reference)", expanded=False):
    st.markdown("**Accuracy** · **Completeness** · **Consistency** · **Timeliness** · **Validity** · **Uniqueness** · **Integrity**")

with st.expander("FAQ / Tips", expanded=False):
    st.markdown(
        """
- **Why can I only see `samples` and `system` catalogs?**  
  Access is filtered by your workspace permissions. Use the refresh button on the Configure page; if still missing, verify Unity Catalog grants.

- **Where are generated YAMLs saved?**  
  To a Volume directory or Workspace Files path you choose. The app will overwrite when re-saving.

- **Can I monitor job progress?**  
  Yes—after triggering, the app polls via the Databricks Jobs API until completion.
        """
    )

# -------------------------
# Footer: centered LMGDATA
# -------------------------
st.markdown('<div class="pf-footer"><div class="lmg-mark">LMGDATA</div></div>', unsafe_allow_html=True)