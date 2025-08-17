# pages/02_Report_Approval.py   (Streamlit multipage)
import os
import pandas as pd
import streamlit as st
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient, Config

st.set_page_config(page_title="Report Approval", layout="wide")
st.title("📊 Report Approval (with Draft Previews)")

CATALOG = os.getenv("CATALOG", "finance")
SCHEMA = os.getenv("SCHEMA", "kyc_ml")
TABLE  = f"{CATALOG}.{SCHEMA}.report_candidates"

cfg = Config()
HOST = (cfg.host or os.getenv("DATABRICKS_HOST","")).replace("https://","").strip("/")
HTTP_PATH = cfg.http_path or os.getenv("DATABRICKS_HTTP_PATH","")
TOKEN     = cfg.token or os.getenv("DATABRICKS_TOKEN","")

if not (HOST and HTTP_PATH and TOKEN):
    st.error("Missing Databricks connection: set DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN.")
    st.stop()

@st.cache_resource
def get_sql_conn():
    return dbsql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=TOKEN)

@st.cache_resource
def get_ws():
    return WorkspaceClient()  # uses same Config (env/passthrough) as above

conn = get_sql_conn()
ws = get_ws()

def fetch_pending():
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, report_name, dataset_view, chart_type, filters, owner, status,
                   export_format, draft_paths, final_paths, submitted_at
            FROM {TABLE}
            WHERE lower(status)='pending'
            ORDER BY submitted_at
        """)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
    return pd.DataFrame(rows, columns=cols)

def dbfs_to_files_url(dbfs_path: str) -> str:
    # dbfs:/FileStore/...  -> https://<host>/files/...
    suffix = dbfs_path.replace("dbfs:/FileStore/","")
    return f"https://{HOST}/files/{suffix}"

def update_status(report_id: str, status: str, reviewed_by: str|None=None, report_url: str|None=None, final_paths: list[str]|None=None):
    sets = [f"status='{status}'"]
    sets.append("decision_at=current_timestamp()")
    if reviewed_by:
        sets.append(f"reviewed_by='{reviewed_by.replace(\"'\",\"''\")}'")
    if report_url is not None:
        sets.append(f"report_url='{report_url.replace(\"'\",\"''\")}'")
    if final_paths is not None:
        arr = ",".join([f\"'{p}'\" for p in final_paths])
        sets.append(f"final_paths=array({arr})")

    with conn.cursor() as cur:
        cur.execute(f"UPDATE {TABLE} SET {', '.join(sets)} WHERE id='{report_id}'")
    conn.commit()

st.caption(f"Catalog: `{CATALOG}` • Schema: `{SCHEMA}` • Table: `{TABLE}`")

df = fetch_pending()
if df.empty:
    st.info("No pending reports.")
    st.stop()

item = st.selectbox(
    "Pick a report candidate",
    df.to_dict("records"),
    format_func=lambda r: f"{r['report_name']} → {r['dataset_view']} (owner: {r['owner']})"
)

st.subheader("Definition")
st.json({
    "report_name": item["report_name"],
    "dataset_view": item["dataset_view"],
    "chart_type": item["chart_type"],
    "filters": item["filters"],
    "export_format": item.get("export_format"),
})

# Show draft links
st.subheader("Draft file(s)")
draft_paths = item.get("draft_paths") or []
if isinstance(draft_paths, str):
    # In some drivers array may come serialized; handle both
    try:
        import ast
        draft_paths = ast.literal_eval(draft_paths)
    except Exception:
        draft_paths = [draft_paths]

if not draft_paths:
    st.warning("No draft files recorded for this candidate yet.")
else:
    for p in draft_paths:
        st.markdown(f"- [{p}]({dbfs_to_files_url(p)})")

# Optional data peek
st.subheader("Dataset peek (first 20)")
try:
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {item['dataset_view']} LIMIT 20")
        data = cur.fetchall(); cols = [c[0] for c in cur.description]
    st.dataframe(pd.DataFrame(data, columns=cols), use_container_width=True)
except Exception as e:
    st.info(f"Peek skipped: {e}")

col1, col2 = st.columns(2)

def strip_draft_suffix(path: str) -> str:
    # Rename ..._DRAFT.ext -> ... .ext
    if "_DRAFT." in path:
        return path.replace("_DRAFT.",".")
    return path

if col1.button("Approve ✅ (Finalize files)"):
    reviewer = os.getenv("REQUESTED_BY") or os.getenv("USER_EMAIL") or "approver"
    final_paths = []
    # Rename/move draft files to remove _DRAFT using DBFS move
    for p in draft_paths:
        src = p.replace("dbfs:/","/dbfs/")  # local path for existence checks
        dst_dbfs = strip_draft_suffix(p)
        # Use SDK to move (works with dbfs:/)
        try:
            ws.dbfs.move(source_path=p, destination_path=dst_dbfs, overwrite=True)
            final_paths.append(dst_dbfs)
        except Exception as e:
            st.error(f"Failed to finalize {p}: {e}")
            st.stop()

    # Build a canonical report_url (first final file if any)
    report_url = dbfs_to_files_url(final_paths[0]) if final_paths else None
    update_status(item["id"], "approved", reviewed_by=reviewer, report_url=report_url, final_paths=final_paths)
    st.success("Approved and finalized. Files renamed (removed _DRAFT).")

if col2.button("Reject ❌"):
    reviewer = os.getenv("REQUESTED_BY") or os.getenv("USER_EMAIL") or "approver"
    update_status(item["id"], "rejected", reviewed_by=reviewer)
    st.success("Rejected.")