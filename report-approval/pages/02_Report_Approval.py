# file: report_approval_ui.py
import streamlit as st
import pandas as pd
from datetime import datetime
from databricks import sql
from databricks.sdk.core import Config
import uuid

cfg = Config()
DATABRICKS_HOST = cfg.host or os.getenv("DATABRICKS_HOST")
HTTP_PATH = "/sql/1.0/warehouses/81e36fef03fb86d0" #os.getenv("DATABRICKS_HTTP_PATH")

@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )

conn = get_connection()


def fetch_pending_reports():
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM finance.kyc_gold.report_candidates WHERE status='PENDING'")
        return pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

def update_status(report_id, status):
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE finance.kyc_gold.report_candidates
            SET status='{status}', decision_at='{datetime.now()}'
            WHERE id='{report_id}'
        """)

st.title("📊 Report Approval UI")

df = fetch_pending_reports()
if df.empty:
    st.info("No pending reports.")
else:
    for _, row in df.iterrows():
        st.subheader(row['report_name'])
        st.text(f"Dataset: {row['dataset_view']}")
        st.text(f"Chart Type: {row['chart_type']}")
        st.text(f"Filters: {row['filters']}")

        col1, col2 = st.columns(2)
        with col1:
            if st.button(f"✅ Approve {row['report_name']}", key=f"approve_{row['id']}"):
                update_status(row['id'], 'approved')
                st.success("Marked for publishing/export")
        with col2:
            if st.button(f"❌ Reject {row['report_name']}", key=f"reject_{row['id']}"):
                update_status(row['id'], 'rejected')
                st.error("Rejected")