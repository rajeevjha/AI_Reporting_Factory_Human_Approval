import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd, uuid, re
from datetime import datetime

try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

CAND_TABLE = 'finance.kyc_gold.ai_sql_candidates'
LOG_TABLE = 'finance.kyc_gold.ai_sql_approval_log'
QUEUE_TABLE = 'finance.kyc_gold.report_export_queue'

def fetch_pending():
    try:
        df = spark.sql(f"SELECT * FROM {CAND_TABLE} WHERE status='PENDING' ORDER BY created_at DESC")
        return df.toPandas() if df.count()>0 else pd.DataFrame()
    except Exception as e:
        st.error(f"Cannot read candidates: {e}")
        return pd.DataFrame()

def preview(sql_text, n=10):
    m = re.search(r'(WITH\\b|SELECT\\b)', sql_text, flags=re.IGNORECASE)
    sql = sql_text[m.start():] if m else sql_text
    try:
        return spark.sql(f"SELECT * FROM ({sql}) tmp LIMIT {n}").toPandas(), None
    except Exception as e:
        return None, str(e)

def log(decision,user,report,sql,notes=None):
    rec=[(str(uuid.uuid4()), report, user, decision, sql, notes or '', datetime.utcnow())]
    cols=['id','report_name','user','decision','sql_text','notes','ts']
    spark.createDataFrame(rec, schema=cols).write.format('delta').mode('append').saveAsTable(LOG_TABLE)

def update_status(cid,status,user,notes=None):
    ts = datetime.utcnow().isoformat()
    spark.sql(f\"\"\"
    MERGE INTO {CAND_TABLE} t
    USING (SELECT '{cid}' as id, '{status}' as status, '{user}' as updated_by, '{ts}' as updated_at, {('NULL' if not notes else repr(notes))} as notes) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.status = s.status, t.updated_by = s.updated_by, t.updated_at = s.updated_at, t.notes = s.notes
    \"\"\")
def create_view(view, sql):
    sql = re.sub(r'\\b(avg|sum|min|max)\\([^\\)]+\\)(?!\\s+AS)', lambda m: f\"{m.group(0)} AS {re.sub('[^0-9a-zA-Z]','_',m.group(0)).lower()}\", sql, flags=re.IGNORECASE)
    spark.sql(f"EXPLAIN {sql}")
    spark.sql(f"CREATE OR REPLACE VIEW {view} AS {sql}")

def enqueue(report, view):
    rec=[(str(uuid.uuid4()), report, view, 'QUEUED', datetime.utcnow(), None, '', [])]
    cols=['id','report_name','view_full_name','status','created_at','finished_at','export_path','notify_emails']
    spark.createDataFrame(rec, schema=cols).write.format('delta').mode('append').saveAsTable(QUEUE_TABLE)

st.set_page_config(page_title='AI SQL Approval (Medallion)', layout='wide')
st.title('AI SQL Approval (Medallion)')
pending = fetch_pending()
st.sidebar.markdown(f'Pending: {len(pending)}')
if pending.empty:
    st.info('No pending candidates.')
else:
    for i,row in pending.iterrows():
        st.header(f\"{row['report_name']} — {row['id']}\")
        st.write('**Prompt:**'); st.text(row['prompt'])
        st.write('**SQL:**')
        sql_box = st.text_area(f\"sql_{row['id']}\", value=row['generated_sql'], height=260)
        if st.button(f'Preview {row['id']}'):
            df,err = preview(sql_box)
            if err: st.error(err)
            else: st.dataframe(df)
        cols = st.columns([1,1,1,4])
        with cols[0]:
            if st.button(f'Approve {row['id']}'):
                user = spark.sql('SELECT current_user()').first()[0]
                view = f\"finance.kyc_gold.{row['report_name']}\"
                try:
                    create_view(view, sql_box)
                    log('APPROVE', user, row['report_name'], sql_box)
                    update_status(row['id'],'APPROVED',user)
                    enqueue(row['report_name'], view)
                    st.success('Approved and queued for export.')
                except Exception as e:
                    st.error(f'Approve failed: {e}')
        with cols[1]:
            if st.button(f'Edit & Approve {row['id']}'):
                user = spark.sql('SELECT current_user()').first()[0]
                view = f\"finance.kyc_gold.{row['report_name']}\"
                try:
                    create_view(view, sql_box)
                    log('EDIT_APPROVE', user, row['report_name'], sql_box)
                    update_status(row['id'],'APPROVED',user,'Edited by approver')
                    enqueue(row['report_name'], view)
                    st.success('Edited & approved.')
                except Exception as e:
                    st.error(f'Edit & Approve failed: {e}')
        with cols[2]:
            if st.button(f'Reject {row['id']}'):
                user = spark.sql('SELECT current_user()').first()[0]
                update_status(row['id'],'REJECTED',user,'Rejected by approver')
                log('REJECT', user, row['report_name'], sql_box)
                st.warning('Rejected.')
        with cols[3]:
            st.write('Created at:', row['created_at'], 'By:', row['created_by'])
