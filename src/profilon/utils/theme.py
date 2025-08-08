# src/profilon/utils/theme.py
from __future__ import annotations
import streamlit as st

# One-to-one CLA brand palette → CSS variables.
# Mirrors CLA section in utils/color.py exactly (names flattened to kebab-case).
_CLA_HEX = {
    # Primary
    "riptide": "#7DD2D3",
    "navy": "#2E334E",
    # Secondary
    "celadon": "#E2E868",
    "saffron": "#FBC55A",
    "scarlett": "#EE5340",
    # Neutrals
    "charcoal": "#25282A",
    "smoke": "#ABAEAB",
    "cloud": "#F7F7F6",
    "white": "#FFFFFF",
    "black": "#000000",
    # Tints
    "riptide-tint-light":  "#C2EAEA",
    "riptide-tint-medium": "#A4DFE0",
    "riptide-tint-dark":   "#95D9DB",
    "celadon-tint-light":  "#F5F7D1",
    "celadon-tint-medium": "#EEF2B2",
    "celadon-tint-dark":   "#E7EC93",
    "saffron-tint-light":  "#FEEECE",
    "saffron-tint-medium": "#FDDC9C",
    "saffron-tint-dark":   "#FCD17B",
    "scarlett-tint-light": "#FBCDC4",
    "scarlett-tint-medium":"#F69B89",
    "scarlett-tint-dark":  "#F37962",
    # Shades
    "riptide-shade-light": "#49BFC1",
    "riptide-shade-medium":"#39A5A7",
    "riptide-shade-dark":  "#24787A",
    "navy-shade-light":    "#262A40",
    "navy-shade-medium":   "#1E2133",
    "navy-shade-dark":     "#171927",
}

# App-wide CSS (dark mode). Keep it restrained and brand-true.
_BASE_CSS = f"""
<style>
  :root {{
    /* CLA palette */
    {"".join([f"--cla-{k}: {v};" for k, v in _CLA_HEX.items()])}
    /* Derived tokens */
    --cla-bg: #111315;                /* deep neutral for dark mode */
    --cla-surface: #16191d;           /* card surface */
    --cla-border: #1f2329;            /* hairlines */
    --cla-text: var(--cla-cloud);     /* high-contrast body text on dark */
    --cla-text-muted: var(--cla-smoke);
    --cla-primary: var(--cla-riptide);
    --cla-accent: var(--cla-navy);
    --cla-accent-strong: var(--cla-navy-shade-dark);
    --cla-cta: var(--cla-saffron);
    --cla-danger: var(--cla-scarlett);
  }}

  html, body, [data-testid="stAppViewContainer"] {{
    background-color: var(--cla-bg);
    color: var(--cla-text);
    font-family: system-ui, -apple-system, Segoe UI, Roboto, Inter, Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;
  }}

  /* Typography scale (dark-friendly, minimal) */
  h1 {{ font-size: 1.6rem; line-height: 1.2; font-weight: 800; color: var(--cla-text); margin: 0.2rem 0 0.6rem; }}
  h2 {{ font-size: 1.25rem; line-height: 1.3; font-weight: 750; color: var(--cla-text); margin: 0.8rem 0 0.4rem; }}
  h3 {{ font-size: 1.05rem; line-height: 1.35; font-weight: 700; color: var(--cla-text); margin: 0.8rem 0 0.3rem; }}
  p, label, span, li {{ font-size: 0.95rem; }}

  /* Section header: left bar uses primary (riptide), text anchored with navy accent */
  .cla-section {{
    border-left: 4px solid var(--cla-primary);
    padding-left: 10px;
    margin: 12px 0 6px 0;
  }}
  .cla-muted {{ color: var(--cla-text-muted); font-weight: 600; }}

  /* Subtle separator */
  .cla-hr {{ height: 1px; background: linear-gradient(90deg, transparent, var(--cla-border), transparent); border: none; margin: 16px 0; }}

  /* Cards */
  div[data-testid="stVerticalBlock"] > div[data-testid="stHorizontalBlock"] > div {{
    background-color: var(--cla-surface);
    border: 1px solid var(--cla-border);
    border-radius: 8px;
    padding: 12px 14px;
  }}

  /* Inputs */
  label, .stSelectbox label, .stTextInput label, .stNumberInput label, .stMultiSelect label {{
    font-weight: 700;
    color: var(--cla-text);
  }}
  .stSelectbox div[data-baseweb="select"]>div,
  .stMultiSelect div[data-baseweb="select"]>div,
  .stTextInput input, .stNumberInput input {{
    background-color: #0e1013 !important;
    color: var(--cla-text) !important;
    border-color: var(--cla-border) !important;
  }}

  /* Buttons (CTA = saffron) */
  .stButton>button {{
    background-color: var(--cla-cta);
    color: #111;
    border: 1px solid var(--cla-saffron-tint-dark);
    font-weight: 800;
    border-radius: 6px;
  }}
  .stButton>button:hover {{ filter: brightness(0.95); }}

  /* Sidebar */
  section[data-testid="stSidebar"] {{
    background-color: #0e1013;
    border-right: 1px solid var(--cla-border);
  }}

  /* Code / YAML */
  pre, code {{ color: var(--cla-cloud); }}

  /* Links w/ riptide emphasis + navy hover */
  a, a:visited {{ color: var(--cla-primary); text-decoration: none; }}
  a:hover {{ color: var(--cla-riptide-shade-light); text-decoration: underline; }}
</style>
"""

def inject_theme() -> None:
    """
    Injects the CLA theme once per session. Safe to call at the top of every page.
    """
    key = "_cla_theme_injected"
    if not st.session_state.get(key):
        st.markdown(_BASE_CSS, unsafe_allow_html=True)
        st.session_state[key] = True
