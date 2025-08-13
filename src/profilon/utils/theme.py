# src/profilon/utils/theme.py

from __future__ import annotations
from importlib.resources import files
import streamlit as st

def inject_theme() -> None:
    """Inject packaged CSS once per page."""
    key = "_profilon_theme_css_injected"
    if st.session_state.get(key):
        return
    css_path = files("profilon") / "assets" / "theme.css"
    st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)
    st.session_state[key] = True