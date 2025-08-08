# notebooks/01_export_preview_job.py  (or jobs/01_export_preview_job.py)

import json, uuid, time, io, csv
from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient

# Widgets populated by Jobs -> notebook_params
dbutils.widgets.text("TABLE_FQN", "")
dbutils.widgets.text("DST_BASE", "/Shared/profylon/previews")  # /Shared... (Workspace Files) or dbfs:/...
dbutils.widgets.text("LIMIT", "1000")
dbutils.widgets.text("COLUMNS", "")        # CSV
dbutils.widgets.text("WHERE", "")          # e.g., "criticality = 'error'"
dbutils.widgets.text("ORDER_BY", "")       # CSV; supports "col desc"
dbutils.widgets.dropdown("FORMAT", "jsonl", ["jsonl", "csv"])

TABLE_FQN = dbutils.widgets.get("TABLE_FQN").strip()
DST_BASE  = dbutils.widgets.get("DST_BASE").strip()
LIMIT     = int(dbutils.widgets.get("LIMIT") or "1000")
COLUMNS   = [c.strip() for c in dbutils.widgets.get("COLUMNS").split(",") if c.strip()] or None
WHERE     = (dbutils.widgets.get("WHERE") or "").strip() or None
ORDER_BY  = [c.strip() for c in dbutils.widgets.get("ORDER_BY").split(",") if c.strip()] or None
FORMAT    = dbutils.widgets.get("FORMAT").lower()

if not TABLE_FQN:
    raise ValueError("TABLE_FQN is required")

run_uuid = str(uuid.uuid4())
out_dir = f"{DST_BASE.rstrip('/')}/{run_uuid}"
wc = WorkspaceClient()

def _upload_bytes(path: str, data: bytes):
    if path.startswith("/"):            # Workspace Files
        wc.files.upload(file_path=path, contents=data, overwrite=True)
    elif path.startswith("dbfs:/") or path.startswith("/dbfs/"):
        dbutils.fs.put(path if path.startswith("dbfs:/") else "dbfs:"+path, data.decode("utf-8"), True)
    else:
        raise ValueError(f"Unsupported DST_BASE path: {path}")

df = spark.table(TABLE_FQN)
if COLUMNS: df = df.select(*COLUMNS)
if WHERE: df = df.where(WHERE)
if ORDER_BY:
    exprs = []
    for token in ORDER_BY:
        t = token.strip()
        if t.lower().endswith(" desc"): exprs.append(F.col(t[:-5].strip()).desc())
        elif t.lower().endswith(" asc"): exprs.append(F.col(t[:-4].strip()).asc())
        else: exprs.append(F.col(t).asc())
    df = df.orderBy(*exprs)

rows = df.limit(int(LIMIT)).collect()

if FORMAT == "csv":
    cols = df.columns
    sio = io.StringIO()
    w = csv.writer(sio); w.writerow(cols)
    for r in rows: w.writerow([r[c] for c in cols])
    payload = sio.getvalue().encode("utf-8"); preview_name = "preview.csv"
else:
    payload = ("\n".join(json.dumps(r.asDict(recursive=True), default=str) for r in rows)).encode("utf-8")
    preview_name = "preview.jsonl"

preview_path = f"{out_dir}/{preview_name}"
_upload_bytes(preview_path, payload)

schema = [{"name": f.name, "type": f.dataType.simpleString(), "nullable": f.nullable} for f in df.schema.fields]
meta = {
    "run_uuid": run_uuid,
    "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    "table": TABLE_FQN,
    "dst_base": DST_BASE,
    "out_dir": out_dir,
    "limit": LIMIT,
    "columns": COLUMNS,
    "where": WHERE,
    "order_by": ORDER_BY,
    "format": FORMAT,
    "schema": schema,
    "rowcount": len(rows),
    "preview_path": preview_path,
}
_upload_bytes(f"{out_dir}/preview_meta.json", json.dumps(meta, indent=2).encode("utf-8"))

print("=== PREVIEW ===")
print(json.dumps(meta, indent=2))
dbutils.notebook.exit(json.dumps(meta))