# src/profilon/pages/1_configure_&_run.py

import os
import time
import uuid
import yaml
import json
import hashlib
import streamlit as st
from typing import List, Dict, Any, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo

# App-wide CLA theme (CSS + variables)
from profilon.utils.theme import inject_theme

# Optional: ANSI Color for terminal/log parity (no effect on HTML)
try:
    from profilon.utils.color import Color as C  # noqa: F401
except Exception:
    C = None

st.set_page_config(page_title="Configure & Run", layout="wide")
inject_theme()  # global CSS once

# ------------------------------------------------------------
# Constants (you can change these defaults later)
# ------------------------------------------------------------
DEFAULT_CHECKS_TABLE = "dq_dev.dqx.generated_checks_config"
DEFAULT_YAML_DIR     = "/Volumes/<catalog>/<schema>/<volume>/dqx_checks"

# ------------------------------------------------------------
# SDK helpers
# ------------------------------------------------------------
def safe_list_catalogs(wc: WorkspaceClient) -> List[str]:
    try:
        return [c.name for c in wc.catalogs.list() if isinstance(c, CatalogInfo)]
    except Exception as e:
        st.info(f"Catalog list unavailable ({e}); type values manually.")
        return []

def safe_list_schemas(wc: WorkspaceClient, catalog: str) -> List[str]:
    try:
        return [s.name for s in wc.schemas.list(catalog_name=catalog) if isinstance(s, SchemaInfo)]
    except Exception as e:
        st.info(f"Schema list unavailable ({e}); type values manually.")
        return []

def safe_list_tables(wc: WorkspaceClient, catalog: str, schema: str) -> List[str]:
    try:
        return [t.name for t in wc.tables.list(catalog_name=catalog, schema_name=schema) if isinstance(t, TableInfo)]
    except Exception as e:
        st.info(f"Table list unavailable ({e}); type values manually.")
        return []

def safe_list_columns(wc: WorkspaceClient, catalog: str, schema: str, table: str) -> List[str]:
    try:
        t = wc.tables.get(full_name=f"{catalog}.{schema}.{table}")
        cols = getattr(t, "columns", None) or []
        return [c.name for c in cols]
    except Exception as e:
        st.info(f"Column list unavailable ({e}); you can continue without column selection.")
        return []

# ------------------------------------------------------------
# small utils
# ------------------------------------------------------------
def validate_volume_path(p: str) -> None:
    if not p or not p.startswith("/Volumes/"):
        raise ValueError("Path must start with /Volumes/<catalog>/<schema>/<volume>/...")

def save_text_overwrite(path: str, text: str) -> None:
    validate_volume_path(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def render_yaml_with_optional_block(cfg: Dict[str, Any]) -> str:
    base = yaml.safe_dump(cfg, sort_keys=False, default_flow_style=False).rstrip() + "\n"
    optional = """
# --- Optional / documented historically; keep for reference ---
# profile_options:
#   include_histograms: false
#   min_length: null
#   max_length: null
#   min_value: null
#   max_value: null
#   profile_types: null
""".lstrip("\n")
    return base + optional

def job_run_now_with_retry(wc: WorkspaceClient, job_id: int, params: Dict[str, str], attempts: int = 3, backoff: float = 1.5):
    last_err = None
    for i in range(attempts):
        try:
            return wc.jobs.run_now(job_id=job_id, notebook_params=params)
        except Exception as e:
            last_err = e
            time.sleep(backoff ** i)
    raise last_err

# ------------------------------------------------------------
# Indeterminate progress visuals while polling the job
# ------------------------------------------------------------
_IND_BAR_CSS = """
<style>
.pf-indeterminate {
  position: relative; height: 6px; border-radius: 999px;
  background: rgba(255,255,255,0.08); overflow: hidden; margin-top: 8px;
}
.pf-indeterminate::before {
  content: ""; position: absolute; left: -40%;
  width: 40%; height: 100%; border-radius: 999px;
  background: linear-gradient(90deg, var(--cla-riptide), var(--cla-riptide-shade-light), var(--cla-riptide));
  animation: pf-sweep 1.4s infinite ease-in-out;
}
@keyframes pf-sweep {
  0%   { left: -40%; }
  50%  { left: 20%;  width: 60%; }
  100% { left: 100%; }
}
</style>
"""
def _indeterminate_bar():
    st.markdown(_IND_BAR_CSS + '<div class="pf-indeterminate"></div>', unsafe_allow_html=True)

def _poll_job_indeterminate(wc: WorkspaceClient, run_id: int, interval_sec: int = 15) -> None:
    with st.status("Polling Databricks job…", state="running", expanded=True) as s:
        _indeterminate_bar()
        last_msg = None
        while True:
            info = wc.jobs.get_run(run_id=run_id)
            life = info.state.life_cycle_state
            result = getattr(info.state, "result_state", None)
            msg = f"{life} / {result or '…'}"
            if msg != last_msg:
                s.write(msg); last_msg = msg
            if life in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
                break
            time.sleep(max(3, int(interval_sec)))
        s.update(label=f"Final: {life}/{result}", state="complete")

# ------------------------------------------------------------
# Sidebar (launch)
# ------------------------------------------------------------
with st.sidebar:
    st.caption("<span class='cla-muted'>Workspace actions</span>", unsafe_allow_html=True)
    job_id = st.text_input("Databricks Job ID", placeholder="1234567890 (required to run)")
    run_button = st.button("Run Job (Save + Trigger)", use_container_width=True)

wc = WorkspaceClient()  # default auth in Databricks Apps

# ------------------------------------------------------------
# Title
# ------------------------------------------------------------
st.markdown(
    "<div class='cla-section'><h1 style='margin:0'>Configure &amp; Run</h1>"
    "<div class='cla-muted' style='margin-top:2px'>Generate DQX checks, save to YAML / Table / Both</div></div>",
    unsafe_allow_html=True,
)
st.markdown("<div class='cla-hr'></div>", unsafe_allow_html=True)

# ====================================================================
# WIZARD — Step 1: Choose Mode & Target (dependent dropdowns)
# ====================================================================
st.header("Step 1 · Target", divider="gray")

mode = st.selectbox("Mode", ["pipeline", "catalog", "schema", "table"], index=0, help="""
Pick what you want to profile:
- pipeline → by Unity Catalog pipeline name
- catalog  → all tables in a catalog
- schema   → all tables in catalog.schema
- table    → one or more fully qualified tables
""")

exclude_pattern = st.text_input("Exclude pattern (optional, e.g. '.tmp_*')", value="")

name_param = ""
columns: Optional[List[str]] = None
catalog = schema = None
selected_tables: List[str] = []

catalogs = safe_list_catalogs(wc)

if mode == "pipeline":
    try:
        pipeline_names = [p.name for p in wc.pipelines.list_pipelines()]
    except Exception as e:
        st.info(f"Pipelines unavailable via SDK ({e}); type manually.")
        pipeline_names = []
    pipeline = st.selectbox("Pipeline", pipeline_names) if pipeline_names else st.text_input("Pipeline", value="my_pipeline")
    name_param = pipeline

elif mode == "catalog":
    catalog = st.selectbox("Catalog", catalogs) if catalogs else st.text_input("Catalog", value="dq_prd")
    name_param = (catalog or "").strip()

elif mode == "schema":
    catalog = st.selectbox("Catalog", catalogs, key="schema_mode_catalog") if catalogs else st.text_input("Catalog", value="dq_prd", key="schema_mode_catalog_txt")
    if catalog:
        schemas = safe_list_schemas(wc, catalog)
        schema = st.selectbox("Schema", schemas, key="schema_mode_schema") if schemas else st.text_input("Schema", value="monitoring", key="schema_mode_schema_txt")
    name_param = f"{catalog}.{schema}" if (catalog and schema) else ""

else:  # table mode
    catalog = st.selectbox("Catalog", catalogs, key="table_mode_catalog") if catalogs else st.text_input("Catalog", value="dq_prd", key="table_mode_catalog_txt")
    if catalog:
        schemas = safe_list_schemas(wc, catalog)
        schema = st.selectbox("Schema", schemas, key="table_mode_schema") if schemas else st.text_input("Schema", value="monitoring", key="table_mode_schema_txt")

    table_names: List[str] = safe_list_tables(wc, catalog, schema) if (catalog and schema) else []
    if table_names:
        selected_tables = st.multiselect(
            "Tables (select none to profile entire schema)",
            options=table_names,
            default=[],
            key="table_mode_tables",
            help="Use ⌘/Ctrl to select multiple tables. Leave empty to include every table in the schema."
        )
    elif catalog and schema:
        st.info("No tables found via SDK; you can still proceed (profiles entire schema).")
        selected_tables = []

    if selected_tables:
        name_param = ",".join(f"{catalog}.{schema}.{t}" for t in selected_tables)
    elif catalog and schema:
        name_param = f"{catalog}.{schema}"  # treat as “all tables in schema”
    else:
        name_param = ""

    # Columns (only when exactly one table)
    if len(selected_tables) == 1:
        all_cols = safe_list_columns(wc, catalog, schema, selected_tables[0])
        columns = st.multiselect("Columns (optional; default = all)", options=all_cols, default=[], key="table_mode_columns") or None
    elif len(selected_tables) != 1:
        columns = None
        st.caption("<span class='cla-muted'>Columns selectable only when exactly one table is chosen.</span>", unsafe_allow_html=True)

# ====================================================================
# WIZARD — Step 2: Output Format → reveal destinations *only* when chosen
# ====================================================================
ready_for_outputs = bool(name_param)
if not ready_for_outputs:
    st.info("Pick a target above to proceed to output options.")
else:
    st.markdown("<div class='cla-hr'></div>", unsafe_allow_html=True)
    st.header("Step 2 · Output", divider="gray")

    output_format = st.selectbox("Where should generated checks be saved?", ["yaml", "table", "both"], index=0)

    # Reveal sinks conditionally
    output_yaml: Optional[str] = None
    output_table: Optional[str] = None

    if output_format in {"yaml", "both"}:
        st.subheader("YAML destination", divider="gray")
        output_yaml = st.text_input(
            "Output YAML folder or file",
            value=DEFAULT_YAML_DIR,
            help="Folder (files named per table) or a single .yaml/.yml file path. Supports /Volumes, /Workspace Files, dbfs:/"
        )

    if output_format in {"table", "both"}:
        st.subheader("Table destination", divider="gray")
        table_choice = st.radio("Choose checks table", ["Use default", "Custom FQN"], horizontal=False)
        if table_choice == "Use default":
            st.text_input("Default checks table (FQN)", value=DEFAULT_CHECKS_TABLE, disabled=True)
            output_table = DEFAULT_CHECKS_TABLE
        else:
            output_table = st.text_input("Custom table FQN (catalog.schema.table)", value=DEFAULT_CHECKS_TABLE)

# ====================================================================
# WIZARD — Step 3: Rule settings & profile options
# ====================================================================
created_by = st.text_input("Created By", value="LMG")
run_config_name = st.text_input("Run Config Name", value="default")
criticality = st.selectbox("Criticality", ["warn", "error"], index=0)

st.subheader("Sampling & Limits", divider="gray")
sample_fraction = st.slider("sample_fraction", 0.0, 1.0, 0.3, 0.05)
sample_seed = st.number_input("sample_seed (0 = None)", value=0, step=1, min_value=0)
limit = st.number_input("limit", min_value=0, value=1000, step=100)

st.subheader("Cleaning & Outliers", divider="gray")
remove_outliers = st.selectbox("remove_outliers", [False, True], index=0)
num_sigmas = st.slider("num_sigmas", 1.0, 5.0, 3.0, 0.5)
trim_strings = st.selectbox("trim_strings", [True, False], index=0)
round_values = st.selectbox("round (min/max rounding)", [True, False], index=0)

st.subheader("Thresholds", divider="gray")
max_null_ratio = st.slider("max_null_ratio", 0.0, 1.0, 0.05, 0.01)
max_empty_ratio = st.slider("max_empty_ratio", 0.0, 1.0, 0.02, 0.01)
distinct_ratio = st.slider("distinct_ratio", 0.0, 1.0, 0.01, 0.01)
max_in_count = st.number_input("max_in_count", min_value=1, value=20, step=1)

profile_options: Dict[str, Any] = {
    "sample_fraction": float(sample_fraction),
    "sample_seed": None if int(sample_seed) == 0 else int(sample_seed),
    "limit": int(limit),
    "remove_outliers": bool(remove_outliers),
    "outlier_columns": [],
    "num_sigmas": float(num_sigmas),
    "max_null_ratio": float(max_null_ratio),
    "trim_strings": bool(trim_strings),
    "max_empty_ratio": float(max_empty_ratio),
    "distinct_ratio": float(distinct_ratio),
    "max_in_count": int(max_in_count),
    "round": bool(round_values),
}

# ====================================================================
# WIZARD — Step 4: Review config & Run
# ====================================================================
st.markdown("<div class='cla-hr'></div>", unsafe_allow_html=True)
st.header("Step 4 · Review & Run", divider="gray")

def build_config_dict() -> Dict[str, Any]:
    created_at = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    cfg: Dict[str, Any] = {
        "mode": mode,
        "name_param": name_param,
        # Output config (support yaml | table | both)
        "output_format": (output_format if ready_for_outputs else None),
        "output_yaml": (output_yaml if ready_for_outputs and output_yaml else None),
        "output_table": (output_table if ready_for_outputs and output_table else None),
        # Back-compat (some runners may still read a single key)
        "output_location": (
            (output_yaml or output_table or "") if ready_for_outputs else ""
        ),
        # Rulegen behavior
        "profile_options": profile_options,
        "exclude_pattern": exclude_pattern or None,
        "created_by": created_by or "unknown",
        "columns": columns,
        "run_config_name": run_config_name,
        "criticality": criticality,
        "yaml_key_order": "custom",
        "include_table_name": True,
        # metadata
        "_snapshot": {
            "created_at": created_at,
            "created_by": created_by or "unknown",
            "app": "profylon",
            "run_uuid": str(uuid.uuid4()),
        },
    }
    raw = yaml.safe_dump(cfg, sort_keys=True, default_flow_style=False)
    cfg["_snapshot"]["fingerprint_sha256"] = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return cfg

cfg = build_config_dict()
preview_yaml = render_yaml_with_optional_block(cfg)

config_path = st.text_input(
    "Config YAML path (Volume file)",
    value="/Volumes/<catalog>/<schema>/<volume>/dqx_rulegen_config.yaml",
    help="Will be OVERWRITTEN on Save / Run.",
)
st.code(preview_yaml, language="yaml")

colA, colB = st.columns([1,1])
with colA:
    if st.button("💾 Save YAML", use_container_width=True):
        try:
            save_text_overwrite(config_path, preview_yaml)
            st.success(f"Saved config to: {config_path}")
        except Exception as e:
            st.error(f"Failed to save config: {e}")

with colB:
    if run_button:
        if not job_id.strip().isdigit():
            st.error("Enter a numeric Job ID in the sidebar.")
        elif not name_param:
            st.error("Complete Step 1 (Target) before running.")
        elif ready_for_outputs and cfg.get("output_format") in {"yaml", "both"} and not cfg.get("output_yaml"):
            st.error("Provide a YAML destination in Step 2.")
        elif ready_for_outputs and cfg.get("output_format") in {"table", "both"} and not cfg.get("output_table"):
            st.error("Provide a checks table FQN in Step 2.")
        else:
            try:
                # Persist config and trigger the job with SDK
                save_text_overwrite(config_path, preview_yaml)
                run = job_run_now_with_retry(
                    wc, job_id=int(job_id), params={"CONFIG_PATH": config_path}, attempts=3
                )
                run_id = getattr(run, "run_id", None)
                st.success(f"Saved and triggered job_id={job_id}. Run ID: {run_id}")

                # Indeterminate spinner + animated bar; poll via SDK (no fake timing)
                _poll_job_indeterminate(wc, run_id=int(run_id), interval_sec=15)
            except Exception as e:
                st.error(f"Failed to trigger job: {e}")