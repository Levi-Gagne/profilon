# src/profilon/app.py

import os
import base64
import streamlit as st
from profilon.utils.theme import inject_theme  # absolute import


def _load_logo_b64(path: str) -> str | None:
    """Return base64 data URI for the logo (or None if not found)."""
    try:
        with open(path, "rb") as fh:
            return base64.b64encode(fh.read()).decode("utf-8")
    except Exception:
        return None


st.set_page_config(page_title="Profilon", layout="wide")
inject_theme()

# --- Company logo (fixed visual size across screens) ---
# Path: Assets/cla_logo_white.png  (all lowercase, as provided)
_LOGO_PATH = os.path.join("Assets", "cla_logo_white.png")
_logo_b64 = _load_logo_b64(_LOGO_PATH)
_logo_img_html = (
    f'<img class="pf-logo" src="data:image/png;base64,{_logo_b64}" alt="CLA logo" />'
    if _logo_b64
    else ""
)
# Fixed width via CSS so it appears the same size regardless of viewport.
st.markdown(
    """
    <style>
      .pf-logo {
        width: 140px;          /* <- keep visual size constant */
        max-width: 140px;
        height: auto;
        display: block;
        margin: 4px auto 8px;  /* center */
        image-rendering: -webkit-optimize-contrast;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Header (centered, with logo) ---
st.markdown(
    f"""
    <div class="pf-hero">
      <div class="pf-hero__inner" style="padding-top: 8px; padding-bottom: 12px;">
        {_logo_img_html}
        <h1 class="pf-hero__title" style="margin: 4px 0 0; color: var(--cla-cloud);">Profilon</h1>
        <div class="pf-hero__motto" style="margin-top: 6px; font-weight: 800;">
          turn on insight, <span class="accent">turn on trust</span>
        </div>
      </div>
    </div>
    <div class="cla-hr"></div>
    """,
    unsafe_allow_html=True,
)

# --- Optional: lightweight landing content / links ---
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
          <li>Colors map 1:1 to CLA tokens (tints/shades included)</li>
        </ul>
        """,
        unsafe_allow_html=True,
    )