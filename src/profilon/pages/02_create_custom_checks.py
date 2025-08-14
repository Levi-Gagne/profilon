import json
import yaml
import base64
from pathlib import Path
import streamlit as st
from typing import Any, Dict, List

from utils.theme import inject_theme
from utils.dqx_functions import DQX_FUNCTIONS
from databricks.sdk import WorkspaceClient

st.set_page_config(page_title="Create Custom Checks", layout="wide")
inject_theme()

wc = WorkspaceClient()

# --- Robust logo loader ---
def _load_logo_b64() -> str | None:
    here = Path(__file__).resolve()
    candidates = [
        here.parent.parent / "assets" / "cla_logo_white.png",  # src/profilon/assets/...
        here.parents[2] / "assets" / "cla_logo_white.png",
        here.parents[2] / "Assets" / "cla_logo_white.png",
    ]
    for p in candidates:
        try:
            with open(p, "rb") as fh:
                return base64.b64encode(fh.read()).decode("utf-8")
        except Exception:
            continue
    return None

_logo_b64 = _load_logo_b64()

# ---------- Header ----------
right_logo = (
    f'<img class="pf-logo" alt="CLA" style="width:110px;height:auto;display:block;margin-left:auto;" '
    f'src="data:image/png;base64,{_logo_b64}"/>' if _logo_b64 else ""
)

st.markdown(
    f"""
    <div style="display:flex;align-items:flex-end;justify-content:space-between;">
      <div>
        <h1 class="pf-hero__title" style="margin:0;font-size:34px">Create Custom Checks</h1>
        <div class="cla-muted" style="margin-top:2px">Compose rules &amp; export YAML</div>
      </div>
      {right_logo}
    </div>
    <div class="cla-hr"></div>
    """,
    unsafe_allow_html=True,
)

# ---------- Session state ----------
if "custom_rules" not in st.session_state:
    st.session_state.custom_rules: List[Dict[str, Any]] = []

# ---------- Helpers ----------
_BOOL_KEYS = {"negate", "trim_strings", "nulls_distinct",
              "check_missing_records", "null_safe_row_matching", "null_safe_column_value_matching"}

_INT_KEYS = {"window_minutes", "min_records_per_window", "lookback_windows", "max_age_minutes", "days", "offset"}

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
    if (raw.startswith("{") and raw.endswith("}")) or (raw.startswith("[") and raw.endswith("]")):
        try:
            return json.loads(raw)
        except Exception:
            return raw
    return raw or None

def _save_yaml_any(path: str, payload: str, wc: WorkspaceClient):
    if path.startswith("/"):
        wc.files.upload(file_path=path, contents=payload.encode("utf-8"), overwrite=True)
    elif path.startswith("dbfs:/") or path.startswith("/dbfs/") or path.startswith("dbfs:"):
        wc.dbfs.put(path if path.startswith("dbfs:") else "dbfs:" + path, contents=payload, overwrite=True)
    else:
        import os
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
    names = [f["Check"] for f in funcs]
    fn_name = st.selectbox("Function", names, index=0 if names else None)
    fn_info = next((f for f in funcs if f["Check"] == fn_name), None)

    if fn_info:
        st.caption(f"<span class='cla-muted'>{fn_info.get('Description','')}</span>", unsafe_allow_html=True)

        args_schema: Dict[str, str] = fn_info.get("Arguments", {}) or {}
        arg_values: Dict[str, Any] = {}
        for arg, desc in args_schema.items():
            default_placeholder = "true/false" if arg in _BOOL_KEYS else ("[a, b]" if arg in _LIST_KEYS else "value")
            raw = st.text_input(f"{arg}", value="", help=desc, placeholder=default_placeholder, key=f"arg_{arg}")
            val = _parse_value(arg, raw)
            if val not in (None, ""):
                arg_values[arg] = val

        if st.button("Add rule", use_container_width=True, type="primary"):
            if not (table_fqn and rule_name and fn_name):
                st.error("Provide Table, Rule name, and select a Function.")
            else:
                rule_obj = {
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
        st.dataframe(
            [{"table": r["table_name"], "name": r["name"], "function": r["check"]["function"]}
             for r in st.session_state.custom_rules],
            use_container_width=True, hide_index=True
        )

        yaml_text = yaml.safe_dump(st.session_state.custom_rules, sort_keys=False, default_flow_style=False)
        st.markdown("<h3>YAML Preview</h3>", unsafe_allow_html=True)
        st.code(yaml_text, language="yaml")

        save_path = st.text_input("Save YAML to (Volume/Workspace/local)", value="/Volumes/<catalog>/<schema>/<volume>/custom_checks.yaml")
        col1, col2 = st.columns(2)
        with col1:
            st.download_button("Download YAML", data=yaml_text.encode("utf-8"), file_name="custom_checks.yaml", use_container_width=True)
        with col2:
            if st.button("Save YAML", use_container_width=True):
                try:
                    _save_yaml_any(save_path, yaml_text, wc)
                    st.success(f"Saved to {save_path}")
                except Exception as e:
                    st.error(f"Could not save: {e}")

        if st.button("Clear cart", use_container_width=True):
            st.session_state.custom_rules = []
            st.success("Cleared.")