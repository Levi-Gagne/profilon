# src/profilon/pages/02_create_custom_checks.py

import os
import json
import yaml
import streamlit as st
from typing import Any, Dict, List

from utils.theme import inject_theme, hero
from utils.dqx_functions import DQX_FUNCTIONS

# Databricks SDK is only used for saving to Workspace/DBFS; make it optional
try:
    from databricks.sdk import WorkspaceClient
except Exception:  # pragma: no cover
    WorkspaceClient = None  # type: ignore

st.set_page_config(page_title="Create Custom Checks — profilon", layout="wide")
inject_theme()

# ---------- Optional Workspace client ----------
wc = None
if WorkspaceClient:
    try:
        wc = WorkspaceClient()
    except Exception as e:
        st.warning(
            "WorkspaceClient could not be initialized. Saves to Workspace/DBFS will be disabled. "
            f"Details: {e}"
        )

# ---------- Header with right-aligned CLA logo ----------
left, right = st.columns([6, 1])
with left:
    hero("Create Custom Checks", "Compose rules &amp; export YAML")
with right:
    st.image("assets/cla_logo_white.png", output_format="PNG", use_column_width=False, width=140)

st.markdown("<div class='cla-hr'></div>", unsafe_allow_html=True)

# ---------- Session state ----------
if "custom_rules" not in st.session_state:
    st.session_state.custom_rules: List[Dict[str, Any]] = []

# ---------- Helpers ----------
_BOOL_KEYS = {
    "negate",
    "trim_strings",
    "nulls_distinct",
    "check_missing_records",
    "null_safe_row_matching",
    "null_safe_column_value_matching",
}
_INT_KEYS = {
    "window_minutes",
    "min_records_per_window",
    "lookback_windows",
    "max_age_minutes",
    "days",
    "offset",
}
_LIST_KEYS = {"columns", "ref_columns", "merge_columns", "group_by", "exclude_columns"}

def _parse_value(key: str, raw: str):
    raw = (raw or "").strip()
    if key in _BOOL_KEYS:
        return raw.lower() in {"true", "t", "1", "yes", "y", "on"}
    if key in _INT_KEYS:
        try:
            return int(raw)
        except Exception:
            return raw
    if key in _LIST_KEYS:
        if raw.startswith("["):
            try:
                return json.loads(raw)
            except Exception:
                pass
        return [s.strip() for s in raw.split(",") if s.strip()]
    # try JSON for dict/list literals
    if (raw.startswith("{") and raw.endswith("}")) or (raw.startswith("[") and raw.endswith("]")):
        try:
            return json.loads(raw)
        except Exception:
            return raw
    return raw or None

def _save_yaml_any(path: str, payload: str):
    if path.startswith("dbfs:/") or path.startswith("/dbfs/") or path.startswith("dbfs:"):
        if not wc:
            raise RuntimeError("WorkspaceClient not available; cannot write to DBFS.")
        target = path if path.startswith("dbfs:") else ("dbfs:" + path.replace("/dbfs/", ""))
        wc.dbfs.put(target, contents=payload, overwrite=True)
        return
    if path.startswith("/"):  # Workspace Files path
        if not wc:
            raise RuntimeError("WorkspaceClient not available; cannot upload to Workspace Files.")
        wc.files.upload(file_path=path, contents=payload.encode("utf-8"), overwrite=True)
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(payload)

# ---------- Wizard ----------
cA, cB = st.columns([2, 1])

with cA:
    st.markdown("<h2 class='cla-section' style='margin-top:0'>Rule scope & metadata</h2>", unsafe_allow_html=True)
    table_fqn = st.text_input("Table (catalog.schema.table)")
    rule_name = st.text_input("Rule name", placeholder="e.g., price_non_negative")
    run_config_name = st.text_input("Run config", value="default")
    criticality = st.selectbox("Criticality", ["warn", "error"], index=0)
    filter_expr = st.text_input("Filter (optional)", placeholder="SQL WHERE clause (without 'WHERE')")

    st.markdown("<div class='cla-hr'></div>", unsafe_allow_html=True)
    st.markdown("<h2 class='cla-section'>Function & arguments</h2>", unsafe_allow_html=True)

    cat_label_map = {"Dataset-level": "dataset-level_checks", "Row-level": "row-level_checks"}
    category_label = st.radio("Category", list(cat_label_map.keys()), horizontal=True)
    category_key = cat_label_map[category_label]

    funcs = DQX_FUNCTIONS.get(category_key, [])
    names = [f.get("Check", "") for f in funcs]
    fn_name = st.selectbox("Function", names, index=0 if names else None)
    fn_info = next((f for f in funcs if f.get("Check") == fn_name), None)

    if fn_info:
        # exact description straight from your dictionary (no paraphrase)
        with st.expander("Function details", expanded=True):
            st.markdown(
                f"<div class='cla-muted' style='line-height:1.4'>{fn_info.get('Description','')}</div>",
                unsafe_allow_html=True,
            )
            if fn_info.get("Arguments"):
                st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                st.markdown("<b>Arguments</b>", unsafe_allow_html=True)
                for k, v in fn_info["Arguments"].items():
                    st.markdown(f"- <code>{k}</code>: {v}", unsafe_allow_html=True)

        # Argument inputs
        args_schema: Dict[str, str] = fn_info.get("Arguments", {}) or {}
        arg_values: Dict[str, Any] = {}
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        for arg, desc in args_schema.items():
            placeholder = (
                "true/false" if arg in _BOOL_KEYS
                else "[col_a, col_b]" if arg in _LIST_KEYS
                else "value / JSON literal"
            )
            raw = st.text_input(
                arg,
                value="",
                help=desc,
                placeholder=placeholder,
                key=f"arg_{category_key}_{fn_name}_{arg}",
            )
            val = _parse_value(arg, raw)
            if val not in (None, ""):
                arg_values[arg] = val

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button("➕ Add rule", use_container_width=True, type="primary"):
            if not (table_fqn and rule_name and fn_name):
                st.error("Provide Table, Rule name, and select a Function.")
            else:
                rule_obj: Dict[str, Any] = {
                    "table_name": table_fqn,
                    "name": rule_name,
                    "criticality": criticality,
                    "run_config_name": run_config_name,
                    "check": {"function": fn_name, "arguments": arg_values or {}},
                }
                if filter_expr.strip():
                    rule_obj["filter"] = filter_expr.strip()
                st.session_state.custom_rules.append(rule_obj)
                st.success(f"Added rule: {rule_name}")

with cB:
    st.markdown("<h2 class='cla-section' style='margin-top:0'>Cart</h2>", unsafe_allow_html=True)
    st.write(f"Rules in cart: **{len(st.session_state.custom_rules)}**")

    if st.session_state.custom_rules:
        # lightweight summary table
        st.dataframe(
            [
                {"table": r.get("table_name"), "name": r.get("name"), "function": r.get("check", {}).get("function")}
                for r in st.session_state.custom_rules
            ],
            use_container_width=True,
            hide_index=True,
        )

        # YAML preview (as a flat list of rule objects for portability)
        yaml_text = yaml.safe_dump(st.session_state.custom_rules, sort_keys=False, default_flow_style=False)
        st.markdown("<h3>YAML Preview</h3>", unsafe_allow_html=True)
        st.code(yaml_text, language="yaml")

        save_path = st.text_input(
            "Save YAML to (Volume/Workspace/local)",
            value="/Volumes/<catalog>/<schema>/<volume>/custom_checks.yaml",
            help="Supports local paths, Workspace files (/... path), or DBFS (dbfs:/...).",
        )

        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                "Download YAML",
                data=yaml_text.encode("utf-8"),
                file_name="custom_checks.yaml",
                use_container_width=True,
            )
        with col2:
            if st.button("💾 Save YAML", use_container_width=True):
                try:
                    _save_yaml_any(save_path, yaml_text)
                    st.success(f"Saved to {save_path}")
                except Exception as e:
                    st.error(f"Could not save: {e}")

        # cart controls
        c1, c2 = st.columns(2)
        with c1:
            if st.button("🗑️ Clear cart", use_container_width=True):
                st.session_state.custom_rules = []
                st.success("Cleared.")
        with c2:
            if st.button("↺ Remove last", use_container_width=True) and st.session_state.custom_rules:
                popped = st.session_state.custom_rules.pop()
                st.info(f"Removed: {popped.get('name')}")