# src/profilon/app.py

import streamlit as st
from utils.theme import inject_theme  # <- per your ask: 'utils.' not 'profilon.utils'
from utils.dqx_functions import DQX_FUNCTIONS

st.set_page_config(page_title="profilon", layout="wide")
inject_theme()

# --- Hero (bordered, brand colors come from CSS; no inline color overrides) ---
st.markdown(
    """
    <div class="pf-hero bordered">
      <div class="pf-hero__inner">
        <h1 class="pf-hero__title" style="text-transform: lowercase;">profilon</h1>
        <div class="pf-hero__motto">
          turn on insight, <span class="accent">turn on trust</span>
        </div>
      </div>
    </div>
    <div class="pf-hr"></div>
    """,
    unsafe_allow_html=True,
)

# --- “What’s in the box” — concise list of checks pulled from one source of truth ---
def _html_list_from_dict(title: str, items: list[dict]) -> str:
    lis = []
    for it in items:
        name = it.get("Check", "")
        desc = it.get("Description", "")
        lis.append(f"<li><b>{name}</b><br><small class='muted'>{desc}</small></li>")
    return f"<h3>{title}</h3><ul>{''.join(lis)}</ul>"

dataset = DQX_FUNCTIONS.get("dataset-level_checks", [])
rowlevel = DQX_FUNCTIONS.get("row-level_checks", [])

c1, c2 = st.columns([1, 1])
with c1:
    st.markdown(_html_list_from_dict("Dataset-level checks", dataset), unsafe_allow_html=True)

with c2:
    st.markdown(_html_list_from_dict("Row-level checks", rowlevel), unsafe_allow_html=True)

st.markdown("<div class='pf-hr'></div>", unsafe_allow_html=True)

# --- CTA to other pages (minimal, keeps landing page clean) ---
st.markdown(
    """
    <div class="pf-grid">
      <div class="pf-card">
        <h3>Configure &amp; Run</h3>
        <p class="muted">Select scope, generate checks, save to YAML/table, and trigger a job.</p>
      </div>
      <div class="pf-card">
        <h3>Create Custom Checks</h3>
        <p class="muted">Compose rules by function & arguments; export clean YAML.</p>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)