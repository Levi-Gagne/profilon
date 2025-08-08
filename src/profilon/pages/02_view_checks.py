# src/profilon/pages/2_view_checks.py

import json, time
import streamlit as st
from databricks.sdk import WorkspaceClient
from profilon.utils.theme import inject_theme

st.set_page_config(page_title="View Checks", layout="wide")
inject_theme()
wc = WorkspaceClient()

st.markdown(
    "<div class='cla-section'><h1 style='margin:0'>Checks Viewer</h1>"
    "<div class='cla-muted' style='margin-top:2px'>Preview rows without a SQL Warehouse (serverless job export)</div></div>"
    "<div class='cla-hr'></div>",
    unsafe_allow_html=True,
)

# ---- Controls ----
c1, c2, c3 = st.columns([2, 2, 1])
with c1:
    job_id = st.text_input("Preview Job ID", value="", help="Databricks Job that runs the preview notebook")
with c2:
    table_fqn = st.text_input("Table FQN", value="dq_dev.dqx.checks_config")
with c3:
    limit = st.number_input("Limit", min_value=1, max_value=100000, value=1000, step=100)

dst_base = st.text_input("Output base (Workspace Files)", value="/Shared/profylon/previews")
where = st.text_input("WHERE (optional)", value="")
order_by = st.text_input("ORDER BY CSV (optional)", value="")  # e.g., "table_name asc, name desc"
fmt = st.selectbox("Format", ["jsonl", "csv"], index=0)

run_id = st.session_state.get("checks_preview_run_id")

def _poll_run(run_id_int: int):
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

def _render_preview(preview_path: str, fmt: str):
    blob = wc.files.download(file_path=preview_path) if preview_path.startswith("/") else wc.dbfs.read("dbfs:"+preview_path).data
    if fmt == "jsonl":
        rows = [json.loads(line) for line in blob.decode("utf-8").splitlines() if line]
    else:
        txt = blob.decode("utf-8").splitlines()
        hdr = [h.strip() for h in txt[0].split(",")] if txt else []
        rows = [dict(zip(hdr, r.split(","))) for r in txt[1:]]
    st.dataframe(rows, use_container_width=True, hide_index=True)
    st.caption(f"Rows loaded: {len(rows)}")
    st.download_button(f"Download preview.{fmt}", data=blob, file_name=f"preview.{fmt}", use_container_width=True)

# ---- Actions ----
cta1, cta2 = st.columns([1,1])
with cta1:
    if st.button("Run Preview", use_container_width=True):
        if not (job_id and table_fqn):
            st.error("Provide Job ID and Table FQN.")
        else:
            params = {
                "TABLE_FQN": table_fqn,
                "DST_BASE": dst_base,
                "LIMIT": str(int(limit)),
                "COLUMNS": "",            # keep empty; you can expose later
                "WHERE": where,
                "ORDER_BY": order_by,
                "FORMAT": fmt,
            }
            run = wc.jobs.run_now(job_id=int(job_id), notebook_params=params)
            st.session_state["checks_preview_run_id"] = getattr(run, "run_id", None)
            st.success(f"Triggered run_id={st.session_state['checks_preview_run_id']}")

with cta2:
    if st.button("Poll Status", use_container_width=True):
        if not st.session_state.get("checks_preview_run_id"):
            st.error("No run_id in session. Click Run Preview first.")
        else:
            _poll_run(int(st.session_state["checks_preview_run_id"]))

st.markdown("<div class='cla-hr'></div>", unsafe_allow_html=True)

# ---- Load + Render from last run ----
if st.session_state.get("checks_preview_run_id"):
    if st.button("Fetch Result", use_container_width=True):
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