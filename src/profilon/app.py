# src/profilon/app.py

import base64
from pathlib import Path

import streamlit as st
from utils.theme import inject_theme  # absolute import


# -------------------------
# Page + theme
# -------------------------
st.set_page_config(page_title="Profilon", layout="wide")
inject_theme()  # CSS variables + base styles


# -------------------------
# Logo loader (robust path resolution)
# - First try package assets: src/profilon/assets/cla_logo_white.png
# - Fallback to repo root:    Assets/cla_logo_white.png
# -------------------------
def _load_logo_b64() -> str | None:
    here = Path(__file__).resolve()
    candidates = [
        here.parent / "assets" / "cla_logo_white.png",            # src/profilon/assets/...
        here.parents[1] / "Assets" / "cla_logo_white.png",        # repo-root/Assets/...
    ]
    for p in candidates:
        try:
            with open(p, "rb") as fh:
                return base64.b64encode(fh.read()).decode("utf-8")
        except Exception:
            continue
    return None


_logo_b64 = _load_logo_b64()

# Consistent logo size (fixed px so it doesn't scale with screen width)
st.markdown(
    """
    <style>
      /* Keep headings on-brand by level */
      [data-testid="stAppViewContainer"] h1,
      [data-testid="stMarkdownContainer"] h1 { color: var(--cla-cloud); font-weight: 900; }
      [data-testid="stAppViewContainer"] h2,
      [data-testid="stMarkdownContainer"] h2 { color: var(--cla-riptide-shade-light); font-weight: 800; }
      [data-testid="stAppViewContainer"] h3,
      [data-testid="stMarkdownContainer"] h3 { color: var(--cla-saffron); font-weight: 800; }
      [data-testid="stAppViewContainer"] h4,
      [data-testid="stMarkdownContainer"] h4 { color: var(--cla-celadon); font-weight: 750; }

      /* Fixed-size logo (same on any screen size) */
      .pf-logo {
        width: 140px;             /* <-- set the one true size here */
        height: auto;
        display: block;
        margin: 0 auto 10px auto; /* centered above the title */
      }
      /* Slight polish for dark UI */
      .pf-logo { filter: drop-shadow(0 1px 2px rgba(0,0,0,.35)); }
    </style>
    """,
    unsafe_allow_html=True,
)

# -------------------------
# Hero header
# -------------------------
if _logo_b64:
    logo_html = f'<img class="pf-logo" alt="CLA logo" src="data:image/png;base64,{_logo_b64}" />'
else:
    # Graceful fallback if file not found
    logo_html = ""

st.markdown(
    f"""
    <div class="pf-hero">
      <div class="pf-hero__inner" style="text-align:center;">
        {logo_html}
        <h1 class="pf-hero__title" style="margin: 4px 0 0; color: var(--cla-cloud);">Profilon</h1>
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
# Landing content
# -------------------------
c1, c2 = st.columns([1, 1])

with c1:
    st.markdown(
        """
        <h3 class="cla-section">Get Started</h3>
        <ul>
          <li>Open <b>Configure &amp; Run</b> to select pipeline, catalog, schema, or tables</li>
          <li>Preview YAML, save to Volumes, and trigger a Databricks job</li>
        </ul>
        """,
        unsafe_allow_html=True,
    )

with c2:
    st.markdown(
        """
        <h3 class="cla-section">Notes</h3>
        <ul>
          <li>Theme is global via <code>profilon.utils.theme.inject_theme()</code></li>
          <li>Headers are color-mapped by level (H1→Cloud, H2→Riptide shade, H3→Saffron, H4→Celadon)</li>
        </ul>
        """,
        unsafe_allow_html=True,
    )