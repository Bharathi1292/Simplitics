import streamlit as st
import pandas as pd
import pyodbc
import plotly.express as px
from datetime import datetime, timedelta

# SQL Server connection function
@st.cache_resource
def init_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=20.166.7.4,5003;"
        "DATABASE=Simplitics;"
        "UID=LiaUser;"
        "PWD=LuciaToday!"
    )
    return pyodbc.connect(conn_str)

# Load filtered data from SQL Server
@st.cache_data
def load_data_from_sql(date_range=None, status=None, work_type=None):
    conn = init_connection()

    query = """
    SELECT id, hostNm, startWorkDttm, endWorkDttm, status, workTp, queueNm, message, lastTouchDttm
    FROM MDJobLogger
    WHERE 1 = 1
    """

    params = []

    if date_range:
        query += " AND startWorkDttm >= ? AND endWorkDttm <= ?"
        params.extend(date_range)

    if status:
        query += " AND status = ?"
        params.append(status)

    if work_type:
        query += " AND workTp = ?"
        params.append(work_type)

    df = pd.read_sql(query, conn, params=params)
    df["startWorkDttm"] = pd.to_datetime(df["startWorkDttm"])
    df["endWorkDttm"] = pd.to_datetime(df["endWorkDttm"])
    return df

# Show function that encapsulates the entire page
def show():
    st.title("Job Logging Analysis")

    # Date filter
    st.subheader("📅 Date Filters")
    filter_option = st.radio(
        "Select a period",
        ['By Date Filter', 'Today', 'Last 24 Hours', 'Last 7 Days'],
        horizontal=True
    )

    date_range = None
    col1, col2 = st.columns(2)
    if filter_option == 'By Date Filter':
        with col1:
            start_date = st.date_input("Start Date", value=datetime.today().date() - timedelta(days=7))
        with col2:
            end_date = st.date_input("End Date", value=datetime.today().date())
        date_range = (start_date, end_date)
    elif filter_option == 'Today':
        date_range = (datetime.combine(datetime.today(), datetime.min.time()),
                      datetime.combine(datetime.today(), datetime.max.time()))
    elif filter_option == 'Last 24 Hours':
        date_range = (datetime.now() - timedelta(hours=24), datetime.now())
    elif filter_option == 'Last 7 Days':
        date_range = (datetime.now() - timedelta(days=7), datetime.now())

    # Load data for filters
    df_for_filter = load_data_from_sql(date_range=date_range)
    if df_for_filter.empty:
        st.warning("⚠️ No data found for selected date range.")
        st.stop()

    # Status and work type filters
    st.subheader("🔍 Status & Work Type Filters")
    available_statuses = ["All"] + sorted(df_for_filter["status"].dropna().unique())
    selected_status = st.selectbox("Job Status", available_statuses)

    available_work_types = ["All"] + sorted(df_for_filter["workTp"].dropna().unique())
    selected_work_type = st.selectbox("Work Type", available_work_types)

    df = load_data_from_sql(
        date_range=date_range,
        status=None if selected_status == "All" else selected_status,
        work_type=None if selected_work_type == "All" else selected_work_type
    )

    if df.empty:
        st.warning("⚠ No data found for selected filters.")
        st.stop()

    # Data preprocessing
    df['status'] = df['status'].str.strip().str.lower()
    df['processing_time'] = (df['endWorkDttm'] - df['startWorkDttm']).dt.total_seconds() / 60
    df = df[df['processing_time'] >= 0]

    # KPI Metrics
    st.subheader("📊 Job Status Summary")
    status_counts = df['status'].value_counts()
    fail_jobs = status_counts.get('fail', 0)
    ok_jobs = status_counts.get('ok', 0)
    failed_jobs = status_counts.get('failed', 0)
    completed_jobs = status_counts.get('completed', 0)
    total_jobs = len(df)

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Fail", fail_jobs)
    col2.metric("OK", ok_jobs)
    col3.metric("Failed", failed_jobs)
    col4.metric("Completed", completed_jobs)
    col5.metric("Total", total_jobs)

    # Job Status Bar Chart
    fig_bar = px.bar(
        status_counts,
        x=status_counts.index,
        y=status_counts.values,
        labels={'x': 'Status', 'y': 'Count'},
        title="📊 Job Status Distribution",
        color=status_counts.index
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # Job Arrival Trends
    df['JobDate'] = df['startWorkDttm'].dt.date
    jobs_per_day = df.groupby('JobDate').size().reset_index(name='JobsArrived')
    jobs_per_day['RollingAvg'] = jobs_per_day['JobsArrived'].rolling(window=7, min_periods=1).mean()

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(px.bar(jobs_per_day, x='JobDate', y='JobsArrived', title="📅 Jobs Per Day",
                               color='JobsArrived'), use_container_width=True)
    with col2:
        st.plotly_chart(px.line(jobs_per_day, x='JobDate', y='RollingAvg', title="📈 7-Day Rolling Avg",
                                markers=True), use_container_width=True)

    # Processing Time KPIs
    st.subheader("⏳ Processing Time Stats")
    col1, col2, col3 = st.columns(3)
    col1.metric("Avg Time (mins)", f"{df['processing_time'].mean():.2f}")
    col2.metric("Min Time (mins)", f"{df['processing_time'].min():.2f}")
    col3.metric("Max Time (mins)", f"{df['processing_time'].max():.2f}")

    # Avg Processing Time Trend
    processing_trend = df.groupby(df['startWorkDttm'].dt.date)['processing_time'].mean().reset_index()
    fig_proc_time = px.line(processing_trend, x='startWorkDttm', y='processing_time',
                            title="📈 Avg Processing Time Over Time", markers=True)
    st.plotly_chart(fig_proc_time, use_container_width=True)

    # Jobs per Hour Bubble Chart
    df['JobHour'] = df['startWorkDttm'].dt.hour
    jobs_per_hour = df.groupby('JobHour').size().reset_index(name='JobCount')
    df = df.merge(jobs_per_hour, on='JobHour', how='left')

    fig_bubble = px.scatter(df, x='JobHour', y='JobCount', size='processing_time', color='workTp',
                            title="🕒 Jobs Per Hour", hover_data=['id', 'startWorkDttm', 'endWorkDttm'],
                            category_orders={'JobHour': list(range(0, 24))})
    fig_bubble.update_layout(xaxis=dict(dtick=1))
    st.subheader("🕒 Hourly Job Distribution")
    st.plotly_chart(fig_bubble, use_container_width=True)

    # Timeline (last 24 hours)
    if 'hostNm' in df.columns:
        df_timeline = df[df['startWorkDttm'] >= datetime.now() - timedelta(hours=24)]
        if not df_timeline.empty:
            fig_timeline = px.scatter(
                df_timeline, x='startWorkDttm', y='hostNm', color='status',
                title="⏲ Host Activity (Last 24 Hours)",
                hover_data=['id', 'queueNm', 'workTp', 'message']
            )
            st.subheader("⏲ Host Timeline (Last 24 Hours)")
            st.plotly_chart(fig_timeline, use_container_width=True)
        else:
            st.warning("⚠ No recent host activity found.")


