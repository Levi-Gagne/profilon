# src/profilon/utils/theme.py
from __future__ import annotations
import streamlit as st

# CLA palette → CSS variables (1:1 with your guide, flattened names)
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

_BASE_CSS = f"""
<style>
  :root {{
    /* === CLA Brand Tokens (hex) === */
    {"".join([f"--cla-{k}: {v};" for k, v in _CLA_HEX.items()])}

    /* === Derived Theme Tokens (dark mode) === */
    --cla-bg: #111315;
    --cla-surface: #16191d;
    --cla-border: #1f2329;
    --cla-text: var(--cla-cloud);
    --cla-text-muted: var(--cla-smoke);
    --cla-primary: var(--cla-riptide);
    --cla-accent: var(--cla-navy);
    --cla-accent-strong: var(--cla-navy-shade-dark);
    --cla-cta: var(--cla-saffron);
    --cla-danger: var(--cla-scarlett);

    /* === Alpha/utility tokens (single source for translucency & gradients) === */
    --cla-alpha-0: 0;
    --cla-alpha-2: 0.02;
    --cla-alpha-4: 0.04;
    --cla-alpha-8: 0.08;

    /* Soft hero background using alpha (no inline rgba elsewhere) */
    --cla-hero-bg: linear-gradient(
      180deg,
      rgba(0, 0, 0, var(--cla-alpha-0)) 0%,
      rgba(255, 255, 255, var(--cla-alpha-2)) 100%
    );

    /* Hairline divider */
    --cla-hr: linear-gradient(90deg, transparent, var(--cla-border), transparent);
  }}

  html, body, [data-testid="stAppViewContainer"] {{
    background-color: var(--cla-bg);
    color: var(--cla-text);
    font-family: system-ui, -apple-system, Segoe UI, Roboto, Inter, Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;
  }}

  /* === Base Typography === */
  h1 {{ font-size: 1.6rem; line-height: 1.2; font-weight: 800; color: var(--cla-text); margin: 0.2rem 0 0.6rem; }}
  h2 {{ font-size: 1.25rem; line-height: 1.3; font-weight: 750; color: var(--cla-text); margin: 0.8rem 0 0.4rem; }}
  h3 {{ font-size: 1.05rem; line-height: 1.35; font-weight: 700; color: var(--cla-text); margin: 0.8rem 0 0.3rem; }}
  p, label, span, li {{ font-size: 0.95rem; }}
  .cla-muted {{ color: var(--cla-text-muted); font-weight: 600; }}

  /* Section accent */
  .cla-section {{ border-left: 4px solid var(--cla-primary); padding-left: 10px; margin: 12px 0 6px 0; }}
  .cla-hr {{ height: 1px; background: var(--cla-hr); border: none; margin: 16px 0; }}

  /* Cards (columns look) */
  div[data-testid="stVerticalBlock"] > div[data-testid="stHorizontalBlock"] > div {{
    background-color: var(--cla-surface);
    border: 1px solid var(--cla-border);
    border-radius: 8px;
    padding: 12px 14px;
  }}

  /* Inputs */
  label, .stSelectbox label, .stTextInput label, .stNumberInput label, .stMultiSelect label {{
    font-weight: 700; color: var(--cla-text);
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

  /* Links */
  a, a:visited {{ color: var(--cla-primary); text-decoration: none; }}
  a:hover {{ color: var(--cla-riptide-shade-light); text-decoration: underline; }}

  /* === Reusable component classes === */
  .pf-hero {{
    display: flex; align-items: center; justify-content: center; text-align: center;
    width: 100%;
    padding: clamp(12px, 2.5vw, 28px) 0;
    margin: 4px 0 10px;
    background: var(--cla-hero-bg);
  }}
  .pf-hero__inner {{ width: min(1200px, 96%); margin: 0 auto; padding: 0 clamp(8px, 2vw, 24px); }}
  .pf-hero__title {{
    margin: 0; font-weight: 900; letter-spacing: 0.3px;
    font-size: clamp(1.75rem, 1.1rem + 2vw, 2.625rem); color: var(--cla-cloud);
  }}
  .pf-hero__motto {{
    margin: 8px 0 0; font-weight: 800;
    font-size: clamp(0.875rem, 0.7rem + 0.5vw, 1.125rem); line-height: 1.25;
    color: var(--cla-riptide-shade-light); word-spacing: 1px;
  }}
  .pf-hero__motto .accent {{ color: var(--cla-riptide); }}

  .pf-hr {{ height: 1px; border: 0; margin: clamp(10px, 2vw, 18px) 0; background: var(--cla-hr); }}

  .pf-grid {{
    display: grid; grid-template-columns: repeat(12, 1fr);
    gap: clamp(10px, 1.8vw, 18px); width: min(1200px, 96%); margin: 0 auto;
  }}
  .pf-card {{
    grid-column: span 6; background: var(--cla-surface);
    border: 1px solid var(--cla-border); border-radius: 10px;
    padding: clamp(12px, 1.6vw, 18px);
  }}
  .pf-card h3 {{ margin: 0 0 8px 0; font-size: clamp(1.05rem, 0.96rem + 0.4vw, 1.25rem); }}
  @media (max-width: 860px) {{
    .pf-card {{ grid-column: span 12; }}
  }}
</style>
"""

def inject_theme() -> None:
    """Inject CLA theme (tokens + reusable components). Safe to call from any page."""
    key = "_cla_theme_injected"
    if not st.session_state.get(key):
        st.markdown(_BASE_CSS, unsafe_allow_html=True)
        st.session_state[key] = True