# src/profilon/utils/theme.py

from __future__ import annotations
import pathlib
import streamlit as st

def inject_theme(css_path: str = "Assets/theme.css") -> None:
    """
    Inject the global CLA stylesheet once per session.
    Keep CSS as the single source of truth for colors/spacing/typography.
    """
    key = "_cla_theme_injected"
    if st.session_state.get(key):
        return
    try:
        css = pathlib.Path(css_path).read_text(encoding="utf-8")
    except Exception:
        css = ""  # fail-soft; app still loads
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
    st.session_state[key] = True