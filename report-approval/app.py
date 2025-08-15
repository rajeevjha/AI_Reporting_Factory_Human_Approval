# app.py
import os
import streamlit as st

st.set_page_config(page_title="Reporting Factory – Approvals", layout="wide")

st.title("🧰 Reporting Factory — Approvals")
st.write("""
Welcome! Use the pages on the left:
- **AI SQL Approval**: review and approve AI-generated SQL.
- **Report Approval**: review and approve report definitions.

""")

st.success("Open a page from the sidebar to get started 👈")