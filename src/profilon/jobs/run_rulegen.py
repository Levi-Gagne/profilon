# src/profilon/jobs/run_rulegen.py
from __future__ import annotations

import os, time, uuid, json
from typing import Any, Dict, Optional
import yaml
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from profilon.jobs.rulegen import RuleGenerator, RulegenConfig

# ---------- IO helpers ----------

def _files_download(path: str) -> bytes:
    wc = WorkspaceClient()
    if path.startswith("/"):
        return wc.files.download(file_path=path)
    if path.startswith("dbfs:/") or path.startswith("/dbfs/") or path.startswith("dbfs:"):
        return wc.dbfs.read(path if path.startswith("dbfs:") else ("dbfs:" + path)).data
    with open(path, "rb") as f:
        return f.read()

def _files_upload(path: str, payload: bytes) -> None:
    wc = WorkspaceClient()
    if path.startswith("/"):
        wc.files.upload(file_path=path, contents=payload, overwrite=True)
    elif path.startswith("dbfs:/") or path.startswith("/dbfs/") or path.startswith("dbfs:"):
        wc.dbfs.put(path if path.startswith("dbfs:") else ("dbfs:" + path), contents=payload, overwrite=True)
    else:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "wb") as f: f.write(payload)

def _join(*parts: str) -> str:
    a = "/".join(s.strip("/") for s in parts if s is not None)
    if parts and parts[0].startswith("/"): return "/" + a
    if parts and parts[0].startswith("dbfs:/"): return "dbfs:/" + a
    return a

# ---------- manifest + status ----------

def _write_status(base_dir: str, status: str, extra: Optional[Dict[str, Any]] = None) -> str:
    path = _join(base_dir, "status.json")
    payload = {"status": status, "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"), **(extra or {})}
    _files_upload(path, json.dumps(payload, indent=2).encode("utf-8"))
    return path

def _write_manifest(base_dir: str, manifest: Dict[str, Any]) -> str:
    path = _join(base_dir, "manifest.json")
    _files_upload(path, json.dumps(manifest, indent=2).encode("utf-8"))
    return path

# ---------- config load ----------

def load_config(config_path: str) -> Dict[str, Any]:
    raw = _files_download(config_path)
    return yaml.safe_load(raw)

# ---------- main orchestrator ----------

def main(
    config_path: str,
    output_base_override: Optional[str] = None,
    per_run_subdir: bool = True,
    fail_on_empty: bool = False,
) -> Dict[str, Any]:
    """
    Execute Rulegen with a YAML config at config_path.
    Writes run artifacts under output_base/run_uuid when output_format=='yaml'.

    Args:
      config_path: Workspace Files (/Shared/...), DBFS (dbfs:/...), or local path.
      output_base_override: optional base dir to replace cfg.output_location for yaml outputs.
      per_run_subdir: if True, create <output_base>/<run_uuid>/... to avoid collisions.
      fail_on_empty: if True, raise if zero checks were produced.

    Returns:
      dict with keys: run_uuid, output_dir, manifest_path, status_path, summary (RunSummary as dict)
    """
    cfg_dict = load_config(config_path)
    run_uuid = cfg_dict.get("_snapshot", {}).get("run_uuid") or str(uuid.uuid4())
    created_at = cfg_dict.get("_snapshot", {}).get("created_at") or time.strftime("%Y-%m-%dT%H:%M:%S%z")

    # Derive output directory for YAML mode
    output_format = cfg_dict["output_format"]
    output_location = cfg_dict["output_location"]

    if output_format == "yaml":
        base = (output_base_override or output_location).rstrip("/")
        output_dir = _join(base, run_uuid) if per_run_subdir else base
        # If original output was a single file, convert to directory for multi-table output
        if output_location.endswith((".yaml", ".yml")):
            output_dir = _join(os.path.dirname(base), run_uuid)
        cfg_dict["output_location"] = output_dir
    else:
        output_dir = ""  # N/A for table mode

    cfg = RulegenConfig(
        mode=cfg_dict["mode"],
        name_param=cfg_dict["name_param"],
        output_format=output_format,
        output_location=cfg_dict["output_location"],
        profile_options=cfg_dict["profile_options"],
        exclude_pattern=cfg_dict.get("exclude_pattern"),
        created_by=cfg_dict.get("created_by", "unknown"),
        columns=cfg_dict.get("columns"),
        run_config_name=cfg_dict.get("run_config_name", "default"),
        criticality=cfg_dict.get("criticality", "warn"),
        yaml_key_order=cfg_dict.get("yaml_key_order", "custom"),
        include_table_name=cfg_dict.get("include_table_name", True),
        run_uuid=run_uuid,
        created_at=created_at,
    )

    # Status: running
    status_path = _write_status(output_dir or output_location, "running", {"run_uuid": run_uuid})

    # Execute
    gen = RuleGenerator(cfg)
    summary = gen.run()

    if fail_on_empty and summary.total_checks == 0:
        _write_status(output_dir or output_location, "failed", {"reason": "no_checks"})
        raise RuntimeError("No checks generated; failing run per fail_on_empty=True.")

    # Manifest (for app to browse/download)
    manifest = {
        "run_uuid": run_uuid,
        "created_at": created_at,
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "mode": cfg.mode,
        "name_param": cfg.name_param,
        "output_format": summary.output_format,
        "output_location": summary.output_location,   # table fqn or output dir
        "output_dir": output_dir,
        "tables": summary.tables,
        "total_checks": summary.total_checks,
        "written_files": summary.written_files,
        "warnings": summary.warnings,
        "errors": summary.errors,
        "config_path": config_path,
    }
    manifest_path = _write_manifest(output_dir or output_location, manifest)

    # Status: completed
    _write_status(output_dir or output_location, "completed", {"run_uuid": run_uuid, "total_checks": summary.total_checks})

    return {
        "run_uuid": run_uuid,
        "output_dir": output_dir,
        "manifest_path": manifest_path,
        "status_path": status_path,
        "summary": manifest,
    }

# Convenience: allow local/Databricks script execution, not just notebook
if __name__ == "__main__":
    # When run as a Python script task (notebook-less), use environment variables
    cfg = os.environ.get("CONFIG_PATH")
    base = os.environ.get("OUTPUT_BASE")  # optional override for yaml outputs
    per_run = os.environ.get("PER_RUN_SUBDIR", "true").lower() == "true"
    fail = os.environ.get("FAIL_ON_EMPTY", "false").lower() == "true"
    if not cfg:
        raise SystemExit("CONFIG_PATH env var is required")
    out = main(config_path=cfg, output_base_override=base, per_run_subdir=per_run, fail_on_empty=fail)
    print(json.dumps(out, indent=2))