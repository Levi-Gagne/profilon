# src/utils/theme.py
from __future__ import annotations
import re
from pathlib import Path
from typing import Dict, Any
import streamlit as st

# Python 3.11+: tomllib is stdlib. Fallback to "toml" if needed.
try:
    import tomllib  # type: ignore
except Exception:
    import toml as tomllib  # type: ignore  # noqa

_CFG_CACHE: Dict[str, Any] | None = None

def _load_config() -> Dict[str, Any]:
    global _CFG_CACHE
    if _CFG_CACHE is not None:
        return _CFG_CACHE

    # Streamlit loads from .streamlit/config.toml (relative to working dir)
    cfg_path = Path(".streamlit") / "config.toml"
    data: Dict[str, Any] = {}
    if cfg_path.exists():
        with cfg_path.open("rb") as f:
            data = tomllib.load(f)
    _CFG_CACHE = data
    return data

def _get(d: Dict[str, Any], path: str, default=None):
    cur = d
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur

def _token(tokens: Dict[str, str], name: str, fallback: str = "#FF00FF") -> str:
    # allow references to built-in theme colors as well
    builtin_map = {
        "primaryColor": _get(_load_config(), "theme.primaryColor"),
        "backgroundColor": _get(_load_config(), "theme.backgroundColor"),
        "secondaryBackgroundColor": _get(_load_config(), "theme.secondaryBackgroundColor"),
        "textColor": _get(_load_config(), "theme.textColor"),
    }
    if name in tokens:
        return tokens[name]
    if name in builtin_map and builtin_map[name]:
        return builtin_map[name]
    return tokens.get(name, fallback)

def inject_theme() -> None:
    """Read .streamlit/config.toml and inject CSS variables + header mapping + component styles."""
    cfg = _load_config()
    tokens: Dict[str, str] = _get(cfg, "profilon.tokens", {}) or {}
    typo: Dict[str, Any] = _get(cfg, "profilon.typography", {}) or {}
    headers: Dict[str, str] = _get(cfg, "profilon.headers", {}) or {}
    hero_cfg: Dict[str, Any] = _get(cfg, "profilon.components.hero", {}) or {}
    box_cfg: Dict[str, Any] = _get(cfg, "profilon.components.box", {}) or {}
    layout: Dict[str, Any] = _get(cfg, "profilon.layout", {}) or {}

    # Build CSS :root vars from tokens
    css_vars = []
    for k, v in tokens.items():
        key = re.sub(r"[^a-z0-9_-]+", "-", k.lower())
        css_vars.append(f"--cla-{key}: {v};")

    # Header colors by level
    h1_col = _token(tokens, headers.get("h1", "cloud"))
    h2_col = _token(tokens, headers.get("h2", "riptide_shade_light"))
    h3_col = _token(tokens, headers.get("h3", "saffron"))
    h4_col = _token(tokens, headers.get("h4", "celadon"))

    # Typography
    headings_family = typo.get("headings_family", "Inter, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif")
    body_family     = typo.get("body_family",     "Inter, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif")
    mono_family     = typo.get("mono_family",     "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace")
    h1_size = typo.get("h1_size", "2rem")
    h2_size = typo.get("h2_size", "1.5rem")
    h3_size = typo.get("h3_size", "1.25rem")
    h4_size = typo.get("h4_size", "1.05rem")
    body_size = typo.get("body_size", "0.95rem")
    code_size = typo.get("code_size", "0.90rem")
    h_weight = typo.get("h_weight", 800)
    body_weight = typo.get("body_weight", 400)
    letter_spacing = typo.get("letter_spacing", "0.01em")
    line_height = typo.get("line_height", "1.4")

    # Hero + Box component tokens
    hero_border = _token(tokens, hero_cfg.get("border_color", "riptide_shade_light"))
    hero_bg     = _token(tokens, hero_cfg.get("bg_color", "navy_shade_dark"))
    hero_radius = int(hero_cfg.get("radius_px", 14))
    hero_bw     = int(hero_cfg.get("border_width_px", 1))
    hero_pad    = hero_cfg.get("padding", "14px 16px")
    hero_shadow = hero_cfg.get("shadow", "0 1px 10px rgba(0,0,0,.28)")

    box_border = _token(tokens, box_cfg.get("border_color", "navy_shade_light"))
    box_bg     = _token(tokens, box_cfg.get("bg_color", "secondaryBackgroundColor"))
    box_radius = int(box_cfg.get("radius_px", 12))
    box_bw     = int(box_cfg.get("border_width_px", 1))
    box_pad    = box_cfg.get("padding", "12px 14px")
    box_shadow = box_cfg.get("shadow", "0 1px 6px rgba(0,0,0,.18)")

    content_max_width = layout.get("content_max_width", "1400px")
    gutter = layout.get("gutter", "14px")
    hr_opacity = float(layout.get("hr_opacity", 0.15))
    text_color = _get(cfg, "theme.textColor") or "#F7F7F6"

    css = f"""
    <style>
      :root {{
        {"".join(css_vars)}
      }}
      /* Layout tweaks */
      .block-container {{
        max-width: {content_max_width};
        padding-left: {gutter};
        padding-right: {gutter};
      }}

      /* Typography */
      html, body, .stMarkdown, .stText, .stCaption {{
        font-family: {body_family};
        font-weight: {body_weight};
        letter-spacing: {letter_spacing};
        font-size: {body_size};
        line-height: {line_height};
        color: {text_color};
      }}
      h1, h2, h3, h4 {{
        font-family: {headings_family};
        font-weight: {h_weight};
        letter-spacing: {letter_spacing};
        margin: .2rem 0 .1rem;
      }}
      h1 {{ color: {h1_col} !important; font-size: {h1_size}; }}
      h2 {{ color: {h2_col} !important; font-size: {h2_size}; }}
      h3 {{ color: {h3_col} !important; font-size: {h3_size}; }}
      h4 {{ color: {h4_col} !important; font-size: {h4_size}; }}

      code, pre, .stCode, .stSyntax {{
        font-family: {mono_family};
        font-size: {code_size};
      }}

      /* Generic HR */
      .cla-hr {{
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,{hr_opacity}), transparent);
        margin: 12px 0 20px;
      }}

      /* Reusable Box */
      .pf-box {{
        border: {box_bw}px solid {box_border};
        border-radius: {box_radius}px;
        background: {box_bg};
        box-shadow: {box_shadow};
        padding: {box_pad};
      }}

      /* Hero */
      .pf-hero {{
        border: {hero_bw}px solid {hero_border};
        border-radius: {hero_radius}px;
        background: {hero_bg};
        box-shadow: {hero_shadow};
        padding: {hero_pad};
      }}

      .accent {{ color: var(--cla-riptide) !important; font-weight: 700; }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

def hero(title: str, subtitle: str = "") -> None:
    """Render a standard hero using the tokens defined in config.toml."""
    st.markdown(
        f"""
        <div class="pf-hero">
          <h1 style="margin:0">{title}</h1>
          {f'<div style="margin-top:6px; font-weight:800; opacity:.9">{subtitle}</div>' if subtitle else ""}
        </div>
        """,
        unsafe_allow_html=True,
    )