import streamlit as st
import databricks.sql as sql
from databricks.sdk.core import Config
import os
import pandas as pd

# -------------------------
# Streamlit Page Setup
# -------------------------
st.set_page_config(page_title="🛡 Approval for AI SQL", layout="wide")
st.title("🛡 AI SQL Approval")

# -------------------------
# Databricks Config & Env Vars
# -------------------------
cfg = Config()
DATABRICKS_HOST = cfg.host or os.getenv("DATABRICKS_HOST")
HTTP_PATH = "/sql/1.0/warehouses/81e36fef03fb86d0" #os.getenv("DATABRICKS_HTTP_PATH")

if not all([DATABRICKS_HOST, HTTP_PATH]):
    st.error("❌ Missing env vars: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH")
    st.stop()

CAT_SCHEMA = 'finance.kyc_gold'
CAND_TABLE = 'finance.kyc_gold.ai_sql_candidates'

# -------------------------
# Databricks SQL Connection
# -------------------------
@st.cache_resource # connection is cached
def get_connection():
    try:
        conn = sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
        )
        return conn
    except Exception as e:
        st.error(f"❌ Failed to connect to Databricks SQL: {e}")
        st.stop()

conn = get_connection()
st.success("✅ Connected to Databricks SQL Warehouse")

# -------------------------
# Fetch Pending SQL Queries for Review
# -------------------------
def fetch_pending_queries():
    query = f"""
    SELECT id, report_name, prompt, generated_sql, status, notes, created_at
    FROM {CAND_TABLE}
    WHERE status = 'PENDING'
    ORDER BY created_at ASC
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=[c[0] for c in cursor.description])
    except Exception as e:
        st.error(f"Error fetching pending queries: {e}")
        return pd.DataFrame()

pending_df = fetch_pending_queries()

if pending_df.empty:
    st.info("✅ No pending AI-generated queries to review.")
    st.stop()

st.subheader("📋 Pending Queries for Approval")
st.dataframe(pending_df)

# -------------------------
# Human Review & Approval
# -------------------------
selected_id = st.selectbox("Select a Query ID to Review", pending_df["id"].tolist())


if selected_id:
    selected_row = pending_df[pending_df["id"] == selected_id].iloc[0]
    st.write("### Generated SQL Preview")
    st.code(selected_row["generated_sql"], language="sql")
    report_name = selected_row["report_name"]
    st.write(f"### Report Name: {report_name}")

    # Show query output preview
    st.write("### 📊 Query Output Preview (first 20 rows)")
    try:
        preview_sql = f"SELECT * FROM ({selected_row['generated_sql']}) LIMIT 20"
        with conn.cursor() as cursor:
            cursor.execute(preview_sql)
            rows = cursor.fetchall()
            if rows:
                preview_df = pd.DataFrame(rows, columns=[c[0] for c in cursor.description])
                st.dataframe(preview_df)
            else:
                st.info("Query returned no rows.")
    except Exception as e:
        st.warning(f"⚠️ Could not preview query output: {e}")

    approval = st.radio(
        "Do you approve this SQL query?",
        options=["Approve", "Reject"],
        index=0,
        horizontal=True
    )

    if st.button("Submit Decision"):
        new_status = "APPROVED" if approval == "Approve" else "REJECTED"
        try:
            with conn.cursor() as cursor:
                # Update approval status
                cursor.execute(f"""
                    UPDATE {CAND_TABLE} 
                    SET status = '{new_status}',
                        reviewed_at = current_timestamp()
                    WHERE id = '{selected_id}'
                """)

            st.success(f"✅ Query {selected_id} marked as {new_status} - created.")
        except Exception as e:
            st.error(f"Error processing decision: {e}")