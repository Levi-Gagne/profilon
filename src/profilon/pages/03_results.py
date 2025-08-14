# src/profilon/pages/2_view_checks.py

import json
import time
import streamlit as st

from utils.theme import inject_theme

# Try to import Databricks SDK without hard-failing the page
try:
    from databricks.sdk import WorkspaceClient  # type: ignore
except Exception:  # pragma: no cover
    WorkspaceClient = None  # type: ignore

st.set_page_config(page_title="View Checks — profilon", layout="wide")
inject_theme()

# Optional Workspace client
wc = None
if WorkspaceClient:
    try:
        wc = WorkspaceClient()
    except Exception as e:
        st.warning(
            "WorkspaceClient could not be initialized. Job actions and Workspace/DBFS reads will be disabled. "
            f"Details: {e}"
        )

# ---------- Header with bordered title + right-aligned CLA logo ----------
left, right = st.columns([6, 1])
with left:
    st.markdown(
        """
        <div style="display:flex;align-items:flex-end;gap:10px;">
          <div class="pf-hero__title-box"
               style="
                 border: 1px solid var(--cla-riptide-shade-medium, #39A5A7);
                 border-radius: 12px;
                 padding: 10px 14px;
                 background: var(--cla-navy-shade-dark, #171927);
                 box-shadow: 0 1px 8px rgba(0,0,0,.25);
               ">
            <h1 class="pf-hero__title" style="margin:0">Checks Viewer</h1>
            <div class="cla-muted" style="margin-top:2px">
              Preview rows without a SQL Warehouse (serverless job export)
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with right:
    # Make sure the path uses the correct casing: assets/
    st.image("assets/cla_logo_white.png", output_format="PNG", use_column_width=False, width=140)

st.markdown("<div class='cla-hr'></div>", unsafe_allow_html=True)

# ---- Controls ----
c1, c2, c3 = st.columns([2, 2, 1])
with c1:
    job_id = st.text_input("Preview Job ID", value="", help="Databricks Job that runs the preview notebook")
with c2:
    table_fqn = st.text_input("Table FQN", value="dq_dev.dqx.checks_config")
with c3:
    limit = st.number_input("Limit", min_value=1, max_value=100000, value=1000, step=100)

dst_base = st.text_input("Output base (Workspace Files)", value="/Shared/profilon/previews")
where = st.text_input("WHERE (optional)", value="")
order_by = st.text_input("ORDER BY CSV (optional)", value="")  # e.g., "table_name asc, name desc"
fmt = st.selectbox("Format", ["jsonl", "csv"], index=0)

run_id = st.session_state.get("checks_preview_run_id")

# ---- Helpers ----
def _poll_run(run_id_int: int):
    if not wc:
        st.error("WorkspaceClient is unavailable.")
        return
    with st.status("Checking job status...", expanded=True) as s:
        last = None
        while True:
            info = wc.jobs.get_run(run_id=run_id_int)
            life = info.state.life_cycle_state
            result = getattr(info.state, "result_state", None)
            msg = f"{life} / {result or '…'}"
            if msg != last:
                s.write(msg)
                last = msg
            if life in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
                break
            time.sleep(2)
        s.update(label=f"Final: {life}/{result}", state="complete")

def _download_workspace_file_bytes(path: str) -> bytes:
    """Handle various SDK response shapes for Workspace files download."""
    resp = wc.files.download(file_path=path)
    for attr in ("contents", "data"):
        buf = getattr(resp, attr, None)
        if isinstance(buf, (bytes, bytearray)):
            return bytes(buf)
    if hasattr(resp, "read"):
        try:
            buf = resp.read()
            if isinstance(buf, (bytes, bytearray)):
                return bytes(buf)
        except Exception:
            pass
    if hasattr(resp, "__iter__"):
        try:
            return b"".join(resp)
        except Exception:
            pass
    if isinstance(resp, (bytes, bytearray)):
        return bytes(resp)
    raise RuntimeError("Unsupported download response type from Workspace files API")

def _render_preview(preview_path: str, fmt: str):
    if not wc:
        st.error("WorkspaceClient is unavailable.")
        return
    if preview_path.startswith("/"):
        blob = _download_workspace_file_bytes(preview_path)
    else:
        dbfs_path = preview_path if preview_path.startswith("dbfs:") else ("dbfs:" + preview_path.lstrip("/"))
        resp = wc.dbfs.read(dbfs_path)
        blob = getattr(resp, "data", None) or b""

    try:
        if fmt == "jsonl":
            rows = [json.loads(line) for line in blob.decode("utf-8").splitlines() if line]
        else:
            txt_lines = blob.decode("utf-8").splitlines()
            if not txt_lines:
                rows = []
            else:
                hdr = [h.strip() for h in txt_lines[0].split(",")]
                rows = [dict(zip(hdr, [c.strip() for c in r.split(",")])) for r in txt_lines[1:]]
        st.dataframe(rows, use_container_width=True, hide_index=True)
        st.caption(f"Rows loaded: {len(rows)}")
        st.download_button(
            f"Download preview.{fmt}",
            data=blob,
            file_name=f"preview.{fmt}",
            use_container_width=True,
        )
    except Exception as e:
        st.error(f"Could not parse preview ({fmt}): {e}")

# ---- Actions ----
cta1, cta2 = st.columns([1, 1])
with cta1:
    run_disabled = wc is None
    if st.button("Run Preview", use_container_width=True, disabled=run_disabled):
        if not wc:
            st.error("WorkspaceClient is unavailable.")
        elif not (job_id and table_fqn):
            st.error("Provide Job ID and Table FQN.")
        else:
            try:
                params = {
                    "TABLE_FQN": table_fqn,
                    "DST_BASE": dst_base,
                    "LIMIT": str(int(limit)),
                    "COLUMNS": "",  # reserved for future
                    "WHERE": where,
                    "ORDER_BY": order_by,
                    "FORMAT": fmt,
                }
                run = wc.jobs.run_now(job_id=int(job_id), notebook_params=params)
                st.session_state["checks_preview_run_id"] = getattr(run, "run_id", None)
                st.success(f"Triggered run_id={st.session_state['checks_preview_run_id']}")
            except Exception as e:
                st.error(f"Could not trigger preview job: {e}")

with cta2:
    if st.button("Poll Status", use_container_width=True, disabled=(wc is None)):
        if not wc:
            st.error("WorkspaceClient is unavailable.")
        elif not st.session_state.get("checks_preview_run_id"):
            st.error("No run_id in session. Click Run Preview first.")
        else:
            _poll_run(int(st.session_state["checks_preview_run_id"]))

st.markdown("<div class='cla-hr'></div>", unsafe_allow_html=True)

# ---- Load + Render from last run ----
if st.session_state.get("checks_preview_run_id"):
    if st.button("Fetch Result", use_container_width=True, disabled=(wc is None)):
        if not wc:
            st.error("WorkspaceClient is unavailable.")
        else:
            try:
                out = wc.jobs.get_run_output(run_id=int(st.session_state["checks_preview_run_id"]))
                result = out.notebook_output and out.notebook_output.result
                meta = json.loads(result) if result else {}
                st.json(meta)
                preview_path = meta.get("preview_path")
                fmt_used = meta.get("format", fmt)
                if preview_path:
                    _render_preview(preview_path, fmt_used)
                else:
                    st.warning("No preview_path in job output yet.")
            except Exception as e:
                st.error(f"Could not fetch preview: {e}")