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
# Asset loaders (robust path resolution, base64 embeds)
# -------------------------
def _load_b64(*rel_parts: str) -> str | None:
    here = Path(__file__).resolve()
    candidates = [
        here.parent / "assets" / Path(*rel_parts),       # src/profilon/assets/...
        here.parents[1] / "assets" / Path(*rel_parts),   # repo-root/assets/...
        here.parents[1] / "Assets" / Path(*rel_parts),   # optional legacy casing
    ]
    for p in candidates:
        try:
            with open(p, "rb") as fh:
                return base64.b64encode(fh.read()).decode("utf-8")
        except Exception:
            continue
    return None

_logo_b64 = _load_b64("cla_logo_white.png")
_bike_b64 = _load_b64("cla_bike.PNG")


# -------------------------
# Page-local polish (banner, focus highlight, footer)
# -------------------------
st.markdown(
    """
    <style>
      /* Ensure top content isn't clipped but keep it tight to the top */
      .block-container { padding-top: 12px; }

      /* Heading color map by level (fallback hexes if theme vars aren't injected yet) */
      [data-testid="stAppViewContainer"] h1,
      [data-testid="stMarkdownContainer"] h1 { color: var(--cla-cloud, #F7F7F6); font-weight: 900; }
      [data-testid="stAppViewContainer"] h2,
      [data-testid="stMarkdownContainer"] h2 { color: var(--cla-riptide-shade-light, #49BFC1); font-weight: 800; }
      [data-testid="stAppViewContainer"] h3,
      [data-testid="stMarkdownContainer"] h3 { color: var(--cla-saffron, #FBC55A); font-weight: 800; }
      [data-testid="stAppViewContainer"] h4,
      [data-testid="stMarkdownContainer"] h4 { color: var(--cla-celadon, #E2E868); font-weight: 750; }

      .accent { color: var(--cla-riptide, #7DD2D3); }

      /* --- Riptide banner (3D tech look) --- */
      .pf-banner {
        position: relative;
        margin: 6px 0 0 0;          /* slight nudge down so header & logo never clip */
        padding: 16px 18px;
        border-radius: 14px;
        background: linear-gradient(135deg,
                    var(--cla-riptide, #7DD2D3) 0%,
                    var(--cla-riptide-shade-light, #49BFC1) 45%,
                    var(--cla-riptide-shade-medium, #39A5A7) 100%);
        box-shadow:
          0 10px 26px rgba(0,0,0,.35),
          inset 0 1px 0 rgba(255,255,255,.18),
          inset 0 -1px 0 rgba(0,0,0,.25);
        border: 1px solid rgba(255,255,255,.10);
      }
      .pf-banner:before {
        content: "";
        position: absolute;
        top: 0; left: 0; right: 0; height: 38%;
        background: linear-gradient(to bottom, rgba(255,255,255,.22), rgba(255,255,255,0));
        border-radius: 14px 14px 0 0;
        pointer-events: none;
      }
      .pf-banner h1 {
        margin: 0;
        color: var(--cla-navy, #2E334E);
        text-shadow: 0 1px 0 rgba(255,255,255,.45);
        font-size: 40px;
        letter-spacing: -0.3px;
      }
      .pf-banner .sub {
        margin-top: 4px;
        color: rgba(0,0,0,.72);
        font-weight: 800;
        text-shadow: 0 1px 0 rgba(255,255,255,.25);
      }

      /* Right-aligned logo in header (slightly smaller to balance title) */
      .pf-logo {
        width: 96px;
        height: auto;
        display: block;
        margin-left: auto;
        filter: drop-shadow(0 1px 2px rgba(0,0,0,.35));
      }

      .pf-hr,
      .cla-hr { height: 1px; background: rgba(255,255,255,.08); margin: 14px 0 16px 0; }

      /* --- Focus/open highlight for inputs (select, multiselect, text, number, slider) --- */
      .stSelectbox div[data-baseweb="select"]:focus-within,
      .stMultiSelect div[data-baseweb="select"]:focus-within,
      .stTextInput:focus-within,
      .stNumberInput:focus-within,
      .stSlider:focus-within {
        box-shadow:
          0 0 0 2px var(--cla-riptide, #7DD2D3),
          0 0 18px rgba(125,210,211,.35) !important;
        border-radius: 10px;
        transition: box-shadow .12s ease-in-out;
      }

      /* Expander “card” styling */
      .pf-expander .st-emotion-cache-1v0mbdj,
      .pf-expander .st-emotion-cache-1o6jk1p {
        background: var(--cla-navy-shade-dark, #171927) !important;
        border: 1px solid rgba(255,255,255,.08);
        border-radius: 10px;
      }
      .pf-expander .st-emotion-cache-ue6h4q {
        border-left: 1px solid rgba(255,255,255,.06);
        border-right: 1px solid rgba(255,255,255,.06);
        border-bottom: 1px solid rgba(255,255,255,.06);
        border-radius: 0 0 10px 10px;
      }

      /* --- Footer image + LMGDATA mark --- */
      .pf-footer {
        display: grid;
        grid-template-columns: 220px 1fr;
        gap: 18px;
        align-items: center;
        padding: 14px 16px;
        border: 1px solid rgba(255,255,255,.08);
        background: linear-gradient(180deg, rgba(255,255,255,.02), rgba(255,255,255,0));
        border-radius: 12px;
      }
      .pf-footer img { width: 100%; height: auto; border-radius: 8px; box-shadow: 0 6px 18px rgba(0,0,0,.35); }

      .lmg-mark {
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        font-size: 28px;
        letter-spacing: 4px;
        color: #0DF;
        text-shadow:
          0 0 6px rgba(0,221,255,.75),
          0 0 16px rgba(0,221,255,.45),
          0 0 36px rgba(0,221,255,.35);
        transform: perspective(400px) translateZ(6px);
      }

      .pf-footer-gap { height: 14px; }
    </style>
    """,
    unsafe_allow_html=True,
)


# -------------------------
# Sidebar clock (browser-side, non-blocking)
# -------------------------
with st.sidebar:
    components.html(
        """
        <div id="lmg-clock"
             style="
               font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
               font-size: 28px; letter-spacing: 2px;
               color: var(--cla-riptide, #7DD2D3);
               text-shadow: 0 0 8px rgba(125,210,211,.6), 0 0 22px rgba(73,191,193,.45);
               padding: 4px 2px 10px 2px; ">
          --
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
        height=60,
    )


# -------------------------
# Hero header (banner + right-aligned logo)
# -------------------------
left, right = st.columns([6, 1])
with left:
    st.markdown(
        """
        <div class="pf-banner">
          <h1>profilon</h1>
          <div class="sub">turn on insight, <span class="accent">turn on trust</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with right:
    if _logo_b64:
        st.markdown(
            f'<img class="pf-logo" alt="CLA logo" src="data:image/png;base64,{_logo_b64}" />',
            unsafe_allow_html=True,
        )

st.markdown("<div class='pf-hr'></div>", unsafe_allow_html=True)


# -------------------------
# Collapsible sections (no emojis)
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
    st.markdown(
        """
**Accuracy** · **Completeness** · **Consistency** · **Timeliness** · **Validity** · **Uniqueness** · **Integrity**
        """
    )

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

st.markdown("<div class='pf-hr'></div>", unsafe_allow_html=True)


# -------------------------
# Footer: bike image + LMGDATA mark
# -------------------------
if _bike_b64:
    st.markdown('<div class="pf-footer-gap"></div>', unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class="pf-footer">
          <img alt="CLA Bike" src="data:image/png;base64,{_bike_b64}"/>
          <div class="lmg-mark">LMGDATA</div>
        </div>
        """,
        unsafe_allow_html=True,
    )