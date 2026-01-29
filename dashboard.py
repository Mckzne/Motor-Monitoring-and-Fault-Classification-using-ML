import streamlit as st
import pandas as pd
import numpy as np
import time
from datetime import datetime
import plotly.express as px
import firebase_admin
from firebase_admin import credentials, firestore
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
import io
from streamlit_autorefresh import st_autorefresh

# -----------------------------
# FIREBASE SETUP
# -----------------------------
if not firebase_admin._apps:
    cred = credentials.Certificate("pmsm-25905-firebase-adminsdk-fbsvc-eb25d9aa87.json")  
    firebase_admin.initialize_app(cred)
db = firestore.client()

st.set_page_config(page_title="PMSM Fault Diagnosis Dashboard", layout="wide")

# -----------------------------
# UTILS
# -----------------------------
@st.cache_data(ttl=10)
def fetch_verdicts():
    """Fetch all verdicts from Firestore"""
    verdicts_ref = db.collection("verdicts").order_by("timestamp", direction=firestore.Query.DESCENDING)
    docs = verdicts_ref.stream()
    data = []
    for doc in docs:
        d = doc.to_dict()
        if "timestamp" in d and d["timestamp"] is not None:
            try:
                d["timestamp"] = d["timestamp"].replace(tzinfo=None)
            except:
                pass
        data.append(d)
    df = pd.DataFrame(data)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def generate_pdf_report(df):
    """Generate PDF summary report"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph("<b>PMSM Fault Diagnosis Report</b>", styles["Title"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles["Normal"]))
    story.append(Spacer(1, 12))

    if df.empty:
        story.append(Paragraph("No data available.", styles["Normal"]))
    else:
        total = len(df)
        avg_conf = df["confidence"].mean() * 100
        uptime_hours = (df["timestamp"].max() - df["timestamp"].min()).total_seconds() / 3600

        story.append(Paragraph(f"Total Samples Processed: {total}", styles["Normal"]))
        story.append(Paragraph(f"Average Confidence: {avg_conf:.2f}%", styles["Normal"]))
        story.append(Paragraph(f"System Uptime: {uptime_hours:.2f} hours", styles["Normal"]))
        story.append(Spacer(1, 12))

        fault_counts = df["fault_label"].value_counts().reset_index()
        fault_counts.columns = ["Fault", "Count"]
        data = [fault_counts.columns.to_list()] + fault_counts.values.tolist()
        table = Table(data)
        story.append(table)
        story.append(Spacer(1, 12))

    doc.build(story)
    buffer.seek(0)
    return buffer


# -----------------------------
# DASHBOARD LAYOUT
# -----------------------------
st.title("⚙️ PMSM Fault Diagnosis Dashboard")

tab1, tab2, tab3, tab4 = st.tabs(["📊 Live Feed", "📈 Analytics", "📍 Locations", "🧾 Reports"])

# -----------------------------
# TAB 1 - LIVE FEED (Auto Refresh)
# -----------------------------
with tab1:
    st.subheader("Real-Time Fault Verdicts")

    # Refresh every 5 seconds
    count = st_autorefresh(interval=5000, key="refresh_counter")

    df = fetch_verdicts()
    if df.empty:
        st.warning("No verdicts yet...")
    else:
        live_df = df[["timestamp", "fault_label", "location", "description", "confidence", "source_file"]].head(15)
        live_df["confidence"] = (live_df["confidence"] * 100).round(2).astype(str) + "%"
        st.dataframe(live_df, use_container_width=True)

# -----------------------------
# TAB 2 - ANALYTICS
# -----------------------------
with tab2:
    st.subheader("Fault Analytics")
    df = fetch_verdicts()

    if df.empty:
        st.warning("No data to display yet.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            fault_counts = df["fault_label"].value_counts().reset_index()
            fault_counts.columns = ["Fault Type", "Count"]
            fig = px.bar(fault_counts, x="Fault Type", y="Count", color="Fault Type", title="Fault Frequency")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.histogram(df, x="confidence", nbins=20, title="Confidence Distribution")
            st.plotly_chart(fig, use_container_width=True)

        st.divider()
        st.subheader("Sensor Data (Time Series)")

        if "features" in df.columns:
            df_features = pd.json_normalize(df["features"])
            df_combined = pd.concat([df[["timestamp"]], df_features], axis=1)
            sensor = st.selectbox("Select Sensor", options=df_features.columns)
            fig = px.line(df_combined, x="timestamp", y=sensor, title=f"{sensor} over Time")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No sensor features available in verdicts.")


# -----------------------------
# TAB 3 - LOCATIONS
# -----------------------------
with tab3:
    st.subheader("Fault Locations")
    df = fetch_verdicts()

    if df.empty:
        st.warning("No data to display yet.")
    else:
        loc_counts = df["location"].value_counts().reset_index()
        loc_counts.columns = ["Location", "Count"]
        fig = px.bar(loc_counts, x="Location", y="Count", color="Location", title="Fault Occurrence by Location")
        st.plotly_chart(fig, use_container_width=True)


# -----------------------------
# TAB 4 - REPORTS
# -----------------------------
with tab4:
    st.subheader("Generate Summary Report")

    df = fetch_verdicts()

    if st.button("🧾 Generate PDF Report"):
        pdf = generate_pdf_report(df)
        st.download_button(
            label="📥 Download Report",
            data=pdf,
            file_name=f"pmsm_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
            mime="application/pdf"
        )
