# src/profilon/jobs/run_rulegen.py

from __future__ import annotations

import os
import json
import uuid
import time
import argparse
from typing import Any, Dict, Optional

import yaml
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from profilon.jobs.rulegen import RuleGenerator, RulegenConfig, RunSummary

# --------------------------
# Tiny FS helpers for Workspace Files / DBFS / local
# --------------------------
def _files_download(path: str) -> bytes:
    wc = WorkspaceClient()
    if path.startswith("/"):
        try:
            return wc.files.download(file_path=path).read()
        except TypeError:
            return wc.files.download(path=path).read()
    if path.startswith("dbfs:/") or path.startswith("/dbfs/") or path.startswith("dbfs:"):
        target = path if path.startswith("dbfs:") else ("dbfs:" + path)
        return wc.dbfs.read(target).data
    with open(path, "rb") as f:
        return f.read()

def _files_upload(path: str, payload: bytes) -> None:
    wc = WorkspaceClient()
    if path.startswith("/"):
        try:
            wc.files.upload(file_path=path, contents=payload, overwrite=True)
        except TypeError:
            wc.files.upload(path=path, contents=payload, overwrite=True)
        return
    if path.startswith("dbfs:/") or path.startswith("/dbfs/") or path.startswith("dbfs:"):
        target = path if path.startswith("dbfs:") else ("dbfs:" + path)
        wc.dbfs.put(target, contents=payload, overwrite=True)
        return
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "wb") as f:
        f.write(payload)

def _join(*parts: str) -> str:
    parts = [p for p in parts if p is not None]
    a = "/".join(s.strip("/") for s in parts)
    if not parts:
        return a
    head = parts[0]
    if head.startswith("/"):
        return "/" + a
    if head.startswith("dbfs:/"):
        return "dbfs:/" + a
    return a

# --------------------------
# Manifest + status writers
# --------------------------
def _write_status(base_dir: str, status: str, extra: Optional[Dict[str, Any]] = None) -> str:
    path = _join(base_dir, "status.json")
    payload = {"status": status, "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"), **(extra or {})}
    _files_upload(path, json.dumps(payload, indent=2).encode("utf-8"))
    return path

def _write_manifest(base_dir: str, manifest: Dict[str, Any]) -> str:
    path = _join(base_dir, "manifest.json")
    _files_upload(path, json.dumps(manifest, indent=2).encode("utf-8"))
    return path

# --------------------------
# Config loader
# --------------------------
def load_config(config_path: str) -> Dict[str, Any]:
    raw = _files_download(config_path)
    return yaml.safe_load(raw)

# --------------------------
# Orchestrator
# --------------------------
def main(
    config_path: str,
    output_base_override: Optional[str] = None,
    per_run_subdir: bool = True,
    fail_on_empty: bool = False,
) -> Dict[str, Any]:
    """
    Execute Rulegen with a YAML config at `config_path`.
    If writing YAML, we drop a small manifest/status alongside (or under output_base/run_uuid).
    """
    # Spark session (ensures we're on cluster when job runs)
    SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    cfg_dict = load_config(config_path) or {}
    snap = (cfg_dict.get("_snapshot") or {})
    run_uuid = snap.get("run_uuid") or str(uuid.uuid4())
    created_at = snap.get("created_at")

    # Flexible sink mapping (back-compat with existing page)
    fmt = (cfg_dict.get("output_format") or "yaml").lower()
    out_yaml = cfg_dict.get("output_yaml")
    out_table = cfg_dict.get("output_table")
    legacy_out = cfg_dict.get("output_location")

    if fmt == "yaml" and not out_yaml:
        out_yaml = legacy_out
    if fmt == "table" and not out_table:
        out_table = legacy_out
    # For 'both', expect explicit out_yaml + out_table in config
    if fmt == "both" and (not out_yaml or not out_table):
        raise ValueError("Config requires both 'output_yaml' and 'output_table' when output_format='both'.")

    # Build RulegenConfig
    cfg = RulegenConfig(
        mode=cfg_dict["mode"],
        name_param=cfg_dict["name_param"],
        output_format=fmt,
        output_location=legacy_out,
        output_yaml=out_yaml,
        output_table=out_table,
        profile_options=cfg_dict.get("profile_options") or {},
        exclude_pattern=cfg_dict.get("exclude_pattern"),
        exclude_prefix_regex=cfg_dict.get("exclude_prefix_regex"),
        created_by=cfg_dict.get("created_by", "unknown"),
        columns=cfg_dict.get("columns"),
        run_config_name=cfg_dict.get("run_config_name", "default"),
        criticality=cfg_dict.get("criticality", "warn"),
        yaml_key_order=cfg_dict.get("yaml_key_order", "custom"),
        include_table_name=cfg_dict.get("include_table_name", True),
        yaml_metadata=cfg_dict.get("yaml_metadata", False),
        run_uuid=run_uuid,
        created_at=created_at,
        table_doc=cfg_dict.get("table_doc") or None,
    )

    gen = RuleGenerator(cfg)
    summary: RunSummary = gen.run()

    # Decide where to drop manifest/status
    # Priority:
    #  1) explicit override
    #  2) output_yaml folder if present
    #  3) a shared default folder in Workspace Files
    default_runs_base = "/Shared/profilon/runs"
    base_dir = output_base_override or summary.output_yaml or default_runs_base
    # if base_dir looks like a file (endswith .yml/.yaml), use its parent
    if base_dir.endswith((".yml", ".yaml")):
        base_dir = base_dir.rsplit("/", 1)[0]
    if per_run_subdir:
        base_dir = _join(base_dir, run_uuid)

    manifest = {
        "config_path": config_path,
        "run_uuid": run_uuid,
        "created_at": created_at,
        "output_format": summary.output_format,
        "output_yaml": summary.output_yaml,
        "output_table": summary.output_table,
        "tables": summary.tables,
        "total_checks": summary.total_checks,
        "written_yaml_files": summary.written_files,
        "warnings": summary.warnings,
        "errors": summary.errors,
    }

    _write_manifest(base_dir, manifest)

    if fail_on_empty and summary.total_checks == 0:
        _write_status(base_dir, "empty", {"reason": "no checks generated"})
    else:
        _write_status(base_dir, "ok")

    return {"base_dir": base_dir, **manifest}

# --------------------------
# CLI / Databricks entry
# --------------------------
if __name__ == "__main__":
    # Support either env vars (Databricks Job with notebook_params) or CLI args
    env_config_path = os.environ.get("CONFIG_PATH")
    env_output_base = os.environ.get("OUTPUT_BASE")
    env_per_run = os.environ.get("PER_RUN_SUBDIR")
    env_fail_empty = os.environ.get("FAIL_ON_EMPTY")

    if env_config_path:
        main(
            config_path=env_config_path,
            output_base_override=env_output_base,
            per_run_subdir=(str(env_per_run).lower() != "false"),
            fail_on_empty=(str(env_fail_empty).lower() == "true"),
        )
    else:
        ap = argparse.ArgumentParser()
        ap.add_argument("--config-path", required=True, help="YAML config path (Workspace Files/DBFS/local)")
        ap.add_argument("--output-base", default=None, help="Folder to drop manifest/status (defaults beside YAML)")
        ap.add_argument("--no-subdir", action="store_true", help="Do not create per-run subfolder")
        ap.add_argument("--fail-on-empty", action="store_true", help="Exit non-zero (and status=empty) if no checks generated")
        args = ap.parse_args()
        main(
            config_path=args.config_path,
            output_base_override=args.output_base,
            per_run_subdir=not args.no_subdir,
            fail_on_empty=args.fail_on_empty,
        )