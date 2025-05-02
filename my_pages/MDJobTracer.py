import streamlit as st
import pandas as pd
import plotly.express as px
import pyodbc
from datetime import datetime, timedelta

def show():
    st.title("📊 MDJobTracer Steps")
    st.write("This is the Load Schedule Steps monitoring section.")

    # Database connection
    def get_connection():
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=20.166.7.4,5003;"
            "DATABASE=Simplitics;"
            "UID=LiaUser;"
            "PWD=LuciaToday!"
        )
        return pyodbc.connect(conn_str)

    # Load Data Function
    @st.cache_data
    def load_data_from_sql(date_range=None):
        query = """
            SELECT Id, LoadDttm, hostNm, queueTech, queueNm, workTp, message, lastTouchDttm
            FROM [Simplitics].[dbo].[MDJobTracer]
        """
        filters = []
        if date_range:
            start_date, end_date = date_range
            filters.append(f"LoadDttm >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}' AND LoadDttm <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'")

        if filters:
            query += " WHERE " + " AND ".join(filters)

        with get_connection() as conn:
            df = pd.read_sql(query, conn)

        df["LoadDttm"] = pd.to_datetime(df["LoadDttm"])
        df["lastTouchDttm"] = pd.to_datetime(df["lastTouchDttm"], errors='coerce')

        return df

    # Main App
    st.title("Job Tracing Analysis")

    # Date Filters
    st.subheader("🔍 Date Filters")
    filter_option = st.radio(
        "Select a period", 
        ['By Date Filter', 'Today', 'Last 24 Hours', 'Last 7 Days'],
        index=0,
        horizontal=True
    )

    date_range = None
    col1, col2 = st.columns(2)
    if filter_option == 'By Date Filter':
        with col1:
            start_date = st.date_input("Start Date", value=datetime.today().date() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End Date", value=datetime.today().date())
            date_range = (start_date, end_date)
    elif filter_option == 'Today':
        today = datetime.today()
        date_range = (today.replace(hour=0, minute=0, second=0), today.replace(hour=23, minute=59, second=59))
    elif filter_option == 'Last 24 Hours':
        date_range = (datetime.now() - timedelta(hours=24), datetime.now())
    elif filter_option == 'Last 7 Days':
        date_range = (datetime.now() - timedelta(days=7), datetime.now())

    # Load Filtered Data
    df = load_data_from_sql(date_range=date_range)

    if df.empty:
        st.warning("⚠️ No data available for the selected filters.")
    else:
        df['message'] = df['message'].astype(str).str.lower()

        # Categorize Hostnames
        df["HostCategory"] = df["hostNm"].apply(
            lambda x: "SQLWorker" if x.startswith("z__sqlworker_CoincapAssets") 
            else "PDQData" if x.startswith("pdqdata") 
            else "Other"
        )

        # Hostname Filter
        st.subheader("🔍 Hostname Category Filter")
        host_category = st.radio(
            "Select Host Type", 
            ["All", "SQLWorker", "PDQData"], 
            index=0,
            horizontal=True
        )

        if host_category != "All":
            df = df[df["HostCategory"] == host_category]

        # Work Type Filter
        st.subheader("🔍 Work Type Filter")
        queue_options = ["All"] + list(df["workTp"].dropna().unique())
        selected_queue = st.selectbox("Select Work Type", queue_options, index=0)
        if selected_queue != "All":
            df = df[df["workTp"] == selected_queue]

        # Max Loading Date
        max_loading_date = df['lastTouchDttm'].max()
        max_loading_date_str = max_loading_date.strftime('%Y-%m-%d') if pd.notna(max_loading_date) else "N/A"

        # KPI Metrics
        col1, col2 = st.columns(2)
        col1.metric("Max Loading Date", max_loading_date_str)
        col2.metric("Total Jobs", len(df))

        # Identify Errors and Warnings
        df['error_flag'] = df['message'].str.contains('error', na=False)
        df['warning_flag'] = df['message'].str.contains('warning', na=False)

        # Group by Queue
        queue_kpi = df.groupby('queueNm').agg(
            errors=('error_flag', 'sum'),
            warnings=('warning_flag', 'sum')
        ).reset_index()

        # Display KPIs
        st.subheader("Queue-based Error & Warning Metrics")
        col1, col2 = st.columns(2)
        col1.metric("Total Errors", queue_kpi['errors'].sum())
        col2.metric("Total Warnings", queue_kpi['warnings'].sum())

        # Bar Chart: Errors & Warnings by Queue
        fig_queue = px.bar(queue_kpi, x='queueNm', y=['errors', 'warnings'],
                           title="📊 Errors & Warnings by Queue",
                           labels={'value': 'Count', 'queueNm': 'Queue Name'},
                           barmode='group')
        st.plotly_chart(fig_queue, use_container_width=True)

        # Raw Data View
        with st.expander("📋 View Raw Data", expanded=False):
            st.dataframe(df)

        # Split into SQLWorker and PDQData
        df_sqlworker = df[df["hostNm"].str.startswith("z__sqlworker_CoincapAssets")]
        df_pdqdata = df[df["hostNm"].str.startswith("pdqdata")]

        if 'JobDate' not in df_sqlworker.columns:
            df_sqlworker["JobDate"] = df_sqlworker["LoadDttm"].dt.date
        if 'JobDate' not in df_pdqdata.columns:
            df_pdqdata["JobDate"] = df_pdqdata["LoadDttm"].dt.date

        # Job Count Over Time Charts
        st.markdown("### 📊 **Job Count Over Time - SQLWorker vs PDQData**")
        col1, col2 = st.columns(2)

        with col1:
            if not df_sqlworker.empty:
                fig_sqlworker = px.bar(
                    df_sqlworker.groupby("JobDate").size().reset_index(name="Count"),
                    x="JobDate", y="Count",
                    title="📊 SQLWorker - Job Count Over Time",
                    labels={"JobDate": "Date", "Count": "Number of Jobs"},
                    color="Count",
                    color_continuous_scale="blues"
                )
                st.plotly_chart(fig_sqlworker, use_container_width=True)

        with col2:
            if not df_pdqdata.empty:
                fig_pdqdata = px.bar(
                    df_pdqdata.groupby("JobDate").size().reset_index(name="Count"),
                    x="JobDate", y="Count",
                    title="📊 PDQData - Job Count Over Time",
                    labels={"JobDate": "Date", "Count": "Number of Jobs"},
                    color="Count",
                    color_continuous_scale="reds"
                )
                st.plotly_chart(fig_pdqdata, use_container_width=True)

        # Compare SQLWorker and PDQData Together
        st.markdown("### 📊 **Compare SQLWorker and PDQData Over Time**")
        if not df.empty:
            df["JobDate"] = df["LoadDttm"].dt.date
            df_sqlworker_grouped = df_sqlworker.groupby("JobDate").size().reset_index(name="SQLWorker_Count")
            df_pdqdata_grouped = df_pdqdata.groupby("JobDate").size().reset_index(name="PDQData_Count")
            df_comparison = pd.merge(df_sqlworker_grouped, df_pdqdata_grouped, on="JobDate", how="outer").fillna(0)

            fig_comparison = px.bar(
                df_comparison, x="JobDate", y=["SQLWorker_Count", "PDQData_Count"],
                title="📊 Comparison of SQLWorker and PDQData Over Time",
                labels={"JobDate": "Date", "value": "Job Count"},
                barmode="group",
                color_discrete_sequence=["blue", "orange"]
            )
            st.plotly_chart(fig_comparison, use_container_width=True)


