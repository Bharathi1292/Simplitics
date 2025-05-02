import streamlit as st
import my_pages.ADTSrcFile
import my_pages.ADTSrcFileLog
import my_pages.MDJobTracer
import my_pages.MDJobLogger
import my_pages.MDLdSchedStats
import my_pages.MDLdSchedStps
import my_pages.DataIngest

# Set page configuration
st.set_page_config(page_title="Simplitics Monitoring Dashboard", layout="wide")

# Use st.query_params to handle page state
params = st.query_params
current_page = params.get("page", "Home")

# Add Back button in the sidebar to navigate to the Home page
if current_page != "Home":
    with st.sidebar:
        st.markdown("""
            <style>
            /* Adjust the width of the sidebar */
            .css-1d391kg {  /* Target the sidebar container */
                width: 100px;  /* Adjust the width here */
            }
            </style>
        """, unsafe_allow_html=True)
        if st.button("🔙 Home"):
            st.query_params.page = "Home"
            st.rerun()

    # Directly call show() on the correct page
    if current_page == "ADTSrcFile":
        my_pages.ADTSrcFile.show()
    elif current_page == "ADTSrcFileLog":
        my_pages.ADTSrcFileLog.show()
    elif current_page == "MDJobTracer":
        my_pages.MDJobTracer.show()
    elif current_page == "MDJobLogger":
        my_pages.MDJobLogger.show()
    elif current_page == "MDLdSchedStats":
        my_pages.MDLdSchedStats.show()
    elif current_page == "MDLdSchedStps":
        my_pages.MDLdSchedStps.show()
    elif current_page == "DataIngest":
        my_pages.DataIngest.show()

else:
    # Home page content with colorful background and buttons
    st.markdown("""
        <style>
        body {
            background: linear-gradient(to bottom right, #f0f2f6, #ffffff);
        }
        .tile-button {
            background-color: #ffffff;
            border: 2px solid #ccc;
            border-radius: 12px;
            padding: 20px;
            text-align: center;
            font-size: 18px;
            font-weight: bold;
            box-shadow: 2px 2px 6px rgba(0,0,0,0.1);
            cursor: pointer;
            transition: all 0.3s ease;
        }
        .tile-button:hover {
            background-color: #f0f8ff;
            border-color: #3399ff;
            color: #0077cc;
        }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("<h1 style='text-align: center; color: #ff6347; font-size: 70px;'> Simplitics LIA Project Dashboard</h1>", unsafe_allow_html=True)

    st.markdown("<h4 style='text-align: center; color: #ffffff;'>Analyze job traces, file loads, and data ingestion trends</h4>", unsafe_allow_html=True)
    st.markdown("---")

    st.markdown("### 🔎 Quick Navigation")

    # Navigation buttons to internal dashboards
    cols = st.columns(4)
    pages = [
        ("ADTSrcFile", "📁 Source Files"),
        ("ADTSrcFileLog", "📝 File Logs"),
        ("MDJobTracer", "🔍 Job Tracer"),
        ("MDJobLogger", "📓 Job Logger"),
        ("MDLdSchedStats", "📈 Load Stats"),
        ("MDLdSchedStps", "📊 Load Steps"),
        ("DataIngest", "🚚 Data Ingest"),
    ]

    for i, (key, label) in enumerate(pages):
        with cols[i % 4]:
            button_html = f'<div class="tile-button">{label}</div>'
            if st.button(label, key=key):
                st.query_params.page = key
                st.rerun()

    st.markdown("---")

    st.markdown("""
    ### 📘 Overview

    This dashboard helps you  to monitor and analyze the **data pipeline** effectively, providing real-time insights into job statuses, 
    file processing, data ingestion, and system health.

    **Key Features:**
    - ✅ **Successful Jobs:** Track how many jobs ran successfully today.
    - ❌ **Failed Jobs:** Quickly identify jobs that failed.
    - 🔍 **Job Tracer:** Dive into job execution details and timelines.
    - 📓 **Job Logger:** Review job logs and error messages.
    - 📈 **Load Stats:** Analyze data load volumes and trends.
    - 🚚 **Data Ingest:** Monitor ingestion pipelines and timings.

    Use the **buttons above** to navigate to different dashboards.
    """)

    st.markdown("<hr style='margin-top: 3rem;'>", unsafe_allow_html=True)
    st.caption("© 2025 Simplitics | LIA Dashboard v1.0")