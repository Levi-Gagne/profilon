# src/profilon/jobs/rulegen.py
from __future__ import annotations

import os
import re
import json
import time
import hashlib
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Literal, Optional, Tuple

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import (
    FileChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
    TableChecksStorageConfig,
    VolumeFileChecksStorageConfig,
)
from pyspark.sql import SparkSession


DOC_SUPPORTED_KEYS = {
    "sample_fraction", "sample_seed", "limit",
    "remove_outliers", "outlier_columns", "num_sigmas",
    "max_null_ratio", "trim_strings", "max_empty_ratio",
    "distinct_ratio", "max_in_count", "round",
}

def _glob_to_regex(glob_pattern: str) -> str:
    if not glob_pattern or not glob_pattern.startswith('.'):
        raise ValueError("Exclude pattern must start with a dot, e.g. '.tmp_*'")
    glob = glob_pattern[1:]
    regex = re.escape(glob).replace(r'\*', '.*')
    return '^' + regex + '$'


@dataclass
class RulegenConfig:
    mode: Literal["pipeline", "catalog", "schema", "table"]
    name_param: str                             # pipeline name or CSV; catalog; catalog.schema; or CSV of FQNs
    output_format: Literal["yaml", "table"]     # yaml files or checks table
    output_location: str                        # folder or file for yaml; catalog.schema.table for table
    profile_options: Dict[str, Any]
    exclude_pattern: Optional[str] = None
    created_by: str = "unknown"
    columns: Optional[List[str]] = None         # only when mode == "table" and single table
    run_config_name: str = "default"
    criticality: Literal["warn", "error"] = "warn"
    yaml_key_order: Literal["engine", "custom"] = "custom"
    include_table_name: bool = True

    # runtime metadata (optional)
    run_uuid: Optional[str] = None
    created_at: Optional[str] = None

    def fingerprint(self) -> str:
        raw = yaml.safe_dump(self.__dict__, sort_keys=True, default_flow_style=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass
class RunSummary:
    tables: List[str] = field(default_factory=list)
    total_checks: int = 0
    output_format: Literal["yaml", "table"] = "yaml"
    output_location: str = ""
    written_files: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class RuleGenerator:
    """
    Thin wrapper around DQX profiler/generator with:
      - discovery (pipeline/catalog/schema/table)
      - exclude pattern
      - save to Workspace Files / DBFS / Volumes / Table
      - strict YAML ordering when requested
    """

    def __init__(self, cfg: RulegenConfig):
        self.cfg = cfg
        self.spark = SparkSession.getActiveSession()
        if not self.spark:
            raise RuntimeError("No active Spark session found. Run inside a Databricks cluster.")

        if self.cfg.output_format not in {"yaml", "table"}:
            raise ValueError("output_format must be 'yaml' or 'table'.")
        if self.cfg.output_format == "yaml" and not self.cfg.output_location:
            raise ValueError("When output_format='yaml', provide output_location (folder or file).")

    # ---------- helpers ----------
    def _profile_call_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        if self.cfg.columns is not None:
            if self.cfg.mode != "table":
                raise ValueError("The 'columns' parameter is only valid in mode='table'.")
            kwargs["cols"] = self.cfg.columns
        if self.cfg.profile_options:
            unknown = sorted(set(self.cfg.profile_options) - DOC_SUPPORTED_KEYS)
            if unknown:
                print(f"[INFO] Profiling options not in current docs (passing through): {unknown}")
            kwargs["options"] = self.cfg.profile_options
        return kwargs

    def _exclude_tables_by_pattern(self, fq_tables: List[str]) -> List[str]:
        if not self.cfg.exclude_pattern:
            return fq_tables
        regex = _glob_to_regex(self.cfg.exclude_pattern)
        pattern = re.compile(regex)
        kept = [fq for fq in fq_tables if not pattern.match(fq.split('.')[-1])]
        print(f"[INFO] Excluded {len(fq_tables) - len(kept)} tables by pattern '{self.cfg.exclude_pattern}'")
        return kept

    def _discover_tables(self) -> List[str]:
        print("\n===== RULEGEN CONFIG =====")
        for k, v in self.cfg.__dict__.items():
            if k in ("profile_options",):
                print(f"{k}: {json.dumps(v, indent=2)}")
            else:
                print(f"{k}: {v}")
        print("==========================\n")

        mode = self.cfg.mode
        discovered: List[str] = []
        if mode == "pipeline":
            print("Discovering pipeline output tables...")
            ws = WorkspaceClient()
            targets = [p.strip() for p in self.cfg.name_param.split(",") if p.strip()]
            pls = list(ws.pipelines.list_pipelines())
            for pipeline_name in targets:
                pl = next((p for p in pls if p.name == pipeline_name), None)
                if not pl:
                    raise RuntimeError(f"Pipeline '{pipeline_name}' not found via SDK.")
                latest_update = pl.latest_updates[0].update_id
                events = ws.pipelines.list_pipeline_events(pipeline_id=pl.pipeline_id, max_results=250)
                pipeline_tables = [
                    getattr(ev.origin, "flow_name", None)
                    for ev in events
                    if getattr(ev.origin, "update_id", None) == latest_update and getattr(ev.origin, "flow_name", None)
                ]
                discovered += [x for x in pipeline_tables if x]

        elif mode == "catalog":
            catalog = self.cfg.name_param.strip()
            schemas = [row.namespace for row in self.spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()]
            for s in schemas:
                tbls = self.spark.sql(f"SHOW TABLES IN {catalog}.{s}").collect()
                discovered += [f"{catalog}.{s}.{row.tableName}" for row in tbls]

        elif mode == "schema":
            if self.cfg.name_param.count(".") != 1:
                raise ValueError("For 'schema' mode, name_param must be catalog.schema")
            catalog, schema = self.cfg.name_param.strip().split(".")
            tbls = self.spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
            discovered = [f"{catalog}.{schema}.{row.tableName}" for row in tbls]

        else:  # table
            tables = [t.strip() for t in self.cfg.name_param.split(",") if t.strip()]
            bad = [t for t in tables if t.count(".") != 2]
            if bad:
                raise ValueError(f"Tables must be fully qualified (catalog.schema.table). Invalid: {bad}")
            discovered = tables

        discovered = sorted(set(self._exclude_tables_by_pattern(discovered)))
        print("[INFO] Final table list:")
        for t in discovered: print(" -", t)
        return discovered

    # ---------- storage config helpers ----------
    @staticmethod
    def _infer_file_storage_config(path: str):
        if path.startswith("/Volumes/"):
            return VolumeFileChecksStorageConfig(location=path)
        if path.startswith("/"):
            return WorkspaceFileChecksStorageConfig(location=path)
        if path.startswith("dbfs:/") or path.startswith("/dbfs/") or path.startswith("dbfs:"):
            # DQX engine handles dbfs via FileChecksStorageConfig
            return FileChecksStorageConfig(location=path if path.startswith("dbfs:") else f"dbfs:{path}")
        return FileChecksStorageConfig(location=os.path.abspath(path))

    @staticmethod
    def _table_storage_config(table_fqn: str, run_config_name: Optional[str] = None, mode: str = "append"):
        return TableChecksStorageConfig(location=table_fqn, run_config_name=run_config_name, mode=mode)

    @staticmethod
    def _write_workspace_file(path: str, payload: bytes) -> None:
        wc = WorkspaceClient()
        wc.files.upload(file_path=path, contents=payload, overwrite=True)

    @staticmethod
    def _write_dbfs(path: str, payload: bytes) -> None:
        wc = WorkspaceClient()
        wc.dbfs.put(path if path.startswith("dbfs:/") else f"dbfs:{path}", contents=payload, overwrite=True)

    def _write_yaml_ordered(self, checks: List[Dict[str, Any]], path: str) -> None:
        yaml_str = yaml.safe_dump(checks, sort_keys=False, default_flow_style=False)
        if path.startswith("/"):
            self._write_workspace_file(path, yaml_str.encode("utf-8"))
        elif path.startswith("dbfs:/") or path.startswith("/dbfs/"):
            self._write_dbfs(path, yaml_str.encode("utf-8"))
        else:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f: f.write(yaml_str)

    # ---------- shaping ----------
    def _dq_constraint_to_check(
        self,
        rule_name: str,
        constraint_sql: str,
        table_name: str,
        criticality: str,
        run_config_name: str,
    ) -> Dict[str, Any]:
        d = {
            "name": rule_name,
            "criticality": criticality,
            "run_config_name": run_config_name,
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": constraint_sql, "name": rule_name},
            },
        }
        return ({"table_name": table_name, **d}) if self.cfg.include_table_name else d

    # ---------- core ----------
    def run(self) -> RunSummary:
        dq_engine = DQEngine(WorkspaceClient())
        profiler = DQProfiler(WorkspaceClient())
        generator = DQDltGenerator(WorkspaceClient())

        tables = self._discover_tables()
        call_kwargs = self._profile_call_kwargs()

        out_format, out_loc = self.cfg.output_format, self.cfg.output_location
        written_files: List[str] = []
        total_checks = 0
        warnings, errors = [], []

        for fq_table in tables:
            try:
                # sanity read to ensure permissions
                self.spark.table(fq_table).limit(1).collect()
            except Exception as e:
                w = f"[WARN] Table not readable: {fq_table} ({e})"
                print(w); warnings.append(w); continue

            try:
                df = self.spark.table(fq_table)
                print(f"[RUN] Profiling: {fq_table}")
                summary_stats, profiles = profiler.profile(df, **call_kwargs)
                rules_dict = generator.generate_dlt_rules(profiles, language="Python_Dict")
            except Exception as e:
                w = f"[WARN] Profiling failed for {fq_table}: {e}"
                print(w); warnings.append(w); continue

            checks: List[Dict[str, Any]] = []
            for rule_name, constraint in (rules_dict or {}).items():
                checks.append(self._dq_constraint_to_check(
                    rule_name=rule_name,
                    constraint_sql=constraint,
                    table_name=fq_table,
                    criticality=self.cfg.criticality,
                    run_config_name=self.cfg.run_config_name,
                ))

            if not checks:
                print(f"[INFO] No checks generated for {fq_table}.")
                continue

            if out_format == "yaml":
                cat, sch, tab = fq_table.split(".")
                # Directory -> {table}.yaml ; or exact file path if provided ending with .yaml
                path = out_loc if out_loc.endswith((".yaml", ".yml")) else f"{out_loc.rstrip('/')}/{tab}.yaml"
                if self.cfg.yaml_key_order == "engine":
                    cfg = self._infer_file_storage_config(path)
                    print(f"[RUN] Saving {len(checks)} checks via DQX to: {path}")
                    dq_engine.save_checks(checks, config=cfg)
                else:
                    print(f"[RUN] Saving {len(checks)} checks (ordered YAML) to: {path}")
                    self._write_yaml_ordered(checks, path)
                written_files.append(path)
                total_checks += len(checks)

            else:  # table
                cfg = self._table_storage_config(table_fqn=out_loc, run_config_name=self.cfg.run_config_name, mode="append")
                print(f"[RUN] Appending {len(checks)} checks to table: {out_loc}")
                dq_engine.save_checks(checks, config=cfg)
                total_checks += len(checks)

        print(f"[RUN] {'Saved' if total_checks else 'No'} checks. Count: {total_checks}")
        return RunSummary(
            tables=tables,
            total_checks=total_checks,
            output_format=out_format,
            output_location=out_loc,
            written_files=written_files,
            warnings=warnings,
            errors=errors,
        )
