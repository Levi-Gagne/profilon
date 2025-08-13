# src/profilon/app.py

import streamlit as st
from utils.theme import inject_theme

st.set_page_config(page_title="Profilon", layout="wide")
inject_theme()

# --- Header (centered) ---
st.markdown(
    """
    <div style="text-align:center; margin-top: 8px; margin-bottom: 14px;">
      <h1 style="margin: 0; color: var(--cla-cloud);">Profilon</h1>
      <div style="margin-top: 6px; font-weight: 700; color: var(--cla-riptide-shade-light);">
        turn on insight, <span style="color: var(--cla-riptide)">turn on trust</span>
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
          <li>Theme is global via <code>profillon.utils.theme.inject_theme()</code></li>
          <li>Colors map 1:1 to CLA tokens (tints/shades included)</li>
        </ul>
        """,
        unsafe_allow_html=True,
    )