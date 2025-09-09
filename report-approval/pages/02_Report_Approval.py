# pages/02_Report_Approval.py   (Streamlit multipage)
import os
import pandas as pd
import streamlit as st
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

st.set_page_config(page_title="Report Approval", layout="wide")
st.title("📊 Report Approval")

CATALOG = os.getenv("CATALOG", "finance")
SCHEMA = os.getenv("SCHEMA", "kyc_gold")
TABLE  = f"{CATALOG}.{SCHEMA}.report_candidates"

cfg = Config()
DATABRICKS_HOST = (cfg.host or os.getenv("DATABRICKS_HOST","")).replace("https://","").strip("/")
HTTP_PATH = "/sql/1.0/warehouses/81e36fef03fb86d0"

if not (DATABRICKS_HOST and HTTP_PATH ):
    st.error("Missing Databricks connection: set DATABRICKS_HOST, DATABRICKS_HTTP_PATH.")
    st.stop()

@st.cache_resource
def get_sql_connection():
    return dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )

@st.cache_resource
def get_ws():
    return WorkspaceClient()  # uses same Config (env/passthrough) as above

conn = get_sql_connection()
ws = get_ws()

def fetch_pending():
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, report_name, view_name,  filters, report_owner, status,
                   export_format, report_url as draft_paths, published_at
            FROM {TABLE}
            WHERE status = 'ready_for_business'
            ORDER BY published_at
        """)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
    return pd.DataFrame(rows, columns=cols)

def filename_only(dbfs_path: str) -> str:
    # dbfs:/FileStore/... -> return just the file name.ext
    return os.path.basename(dbfs_path.replace("dbfs:/", "/"))

def dbfs_to_files_url(dbfs_path: str) -> str:
    # dbfs:/FileStore/...  -> https://<host>/files/...
    suffix = dbfs_path.replace("dbfs:/FileStore/","")
    return f"https://{DATABRICKS_HOST}/{suffix}"

def update_status(report_id: str, status: str, reviewed_by: str):
    sets = [f"status='{status}'"]
    sets.append("reviewed_at=current_timestamp()")
    if reviewed_by:
        sets.append(f"reviewed_by='{reviewed_by}'")

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
    format_func=lambda r: f"{r['report_name']} → {r['view_name']} (report_owner: {r['report_owner']})"
)

st.subheader("Definition")
st.json({
    "report_name": item["report_name"],
    "view_name": item["view_name"],
    "filters": item["filters"],
    "export_format": item.get("export_format"),
})

# Show draft links
st.subheader("Draft Report")
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
        name = filename_only(p)
        url  = dbfs_to_files_url(p)
        # Show only the name as a clickable link (URL hidden)
        st.markdown(f"- [{name}]({url})")

# Optional data peek
st.subheader("Dataset Preview (first 20)")
try:
    with conn.cursor() as cur:
        report_view_name = f"{CATALOG}.{SCHEMA}.{item['view_name']}"
        cur.execute(f"SELECT * FROM {report_view_name} LIMIT 20")
        data = cur.fetchall(); cols = [c[0] for c in cur.description]
    st.dataframe(pd.DataFrame(data, columns=cols), use_container_width=True)
except Exception as e:
    st.info(f"Peek skipped: {e}")

col1, col2 = st.columns(2)


if col1.button("Approve Report ✅"):
    reviewer = os.getenv("REQUESTED_BY") or os.getenv("USER_EMAIL") or "approver"
    update_status(item["id"], "APPROVED", reviewed_by=reviewer)
    st.success("Approved and finalized.")

if col2.button("Reject Report ❌"):
    reviewer = os.getenv("REQUESTED_BY") or os.getenv("USER_EMAIL") or "approver"
    update_status(item["id"], "REJECTED", reviewed_by=reviewer)
    st.success("Rejected.")