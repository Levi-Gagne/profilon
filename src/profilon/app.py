import base64
from pathlib import Path

import streamlit as st
from utils.theme import inject_theme  # keep short import path


# -------------------------
# Page + theme
# -------------------------
st.set_page_config(page_title="profilon", layout="wide")
inject_theme()  # CSS variables + base styles


# -------------------------
# Logo loader (robust path resolution)
# -------------------------
def _load_logo_b64() -> str | None:
    here = Path(__file__).resolve()
    candidates = [
        here.parent / "assets" / "cla_logo_white.png",       # src/profilon/assets/...
        here.parents[1] / "assets" / "cla_logo_white.png",   # repo-root/assets/...
        here.parents[1] / "Assets" / "cla_logo_white.png",   # optional legacy casing
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
# Page-local polish
# -------------------------
st.markdown(
    """
    <style>
      /* Nudge the whole page up a bit */
      .block-container { padding-top: 6px; }

      /* Heading color map by level (fallbacks in case global CSS isn't loaded) */
      [data-testid="stAppViewContainer"] h1,
      [data-testid="stMarkdownContainer"] h1 { color: var(--cla-cloud, #F7F7F6); font-weight: 900; }
      [data-testid="stAppViewContainer"] h2,
      [data-testid="stMarkdownContainer"] h2 { color: var(--cla-riptide-shade-light, #49BFC1); font-weight: 800; }
      [data-testid="stAppViewContainer"] h3,
      [data-testid="stMarkdownContainer"] h3 { color: var(--cla-saffron, #FBC55A); font-weight: 800; }
      [data-testid="stAppViewContainer"] h4,
      [data-testid="stMarkdownContainer"] h4 { color: var(--cla-celadon, #E2E868); font-weight: 750; }

      .accent { color: var(--cla-riptide, #7DD2D3); }

      /* Fixed-size logo (slightly smaller) */
      .pf-logo {
        width: 110px;
        height: auto;
        display: block;
        margin: 0 auto 8px auto; /* centered above the title */
        filter: drop-shadow(0 1px 2px rgba(0,0,0,.35));
      }

      /* Hero wrapper + bordered title box */
      .pf-hero { margin-top: 0; }
      .pf-hero__box {
        display: inline-block;
        border: 1px solid var(--cla-riptide-shade-medium, #39A5A7);
        border-radius: 12px;
        padding: 12px 18px;
        background: var(--cla-navy-shade-dark, #171927);
        box-shadow: 0 2px 10px rgba(0,0,0,.25), inset 0 1px 0 rgba(255,255,255,.03);
      }
      .pf-hero__title { font-size: 42px; letter-spacing: -0.25px; }

      .pf-hr,
      .cla-hr { height: 1px; background: rgba(255,255,255,.08); margin: 12px 0 16px 0; }

      /* Expander “card” styling (no emojis) */
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
    </style>
    """,
    unsafe_allow_html=True,
)


# -------------------------
# Hero header
# -------------------------
logo_html = (
    f'<img class="pf-logo" alt="CLA logo" src="data:image/png;base64,{_logo_b64}" />'
    if _logo_b64 else ""
)

st.markdown(
    f"""
    <div class="pf-hero" style="text-align:center;">
      {logo_html}
      <div class="pf-hero__box">
        <h1 class="pf-hero__title" style="margin: 4px 0 0;">profilon</h1>
        <div class="pf-hero__motto" style="margin-top: 6px; font-weight: 800;">
          turn on insight, <span class="accent">turn on trust</span>
        </div>
      </div>
    </div>
    <div class="pf-hr"></div>
    """,
    unsafe_allow_html=True,
)


# -------------------------
# Collapsible sections (no emoji icons)
# -------------------------
with st.expander("Getting started", expanded=False):
    st.markdown(
        """
- **Configure & Run**: choose *pipeline / catalog / schema / table*, set profile options, and save/trigger the job.
- **Create Custom Checks**: compose rule objects with function + arguments; export to YAML.
- **View Checks**: run a lightweight preview job and fetch results without a SQL Warehouse.
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