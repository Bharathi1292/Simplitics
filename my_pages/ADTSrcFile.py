import streamlit as st
import pandas as pd
import plotly.express as px
import pyodbc
from datetime import datetime, timedelta

def show():
    st.title("📁 ADTSrcFile Analysis")

    # --- Date Filters ---
    st.subheader("🗓️ Date Filters")
    filter_option = st.radio("Select a period", ['By Date Filter', 'Today', 'Last 24 Hours', 'Last 7 Days'], horizontal=True)

    date_range = None
    min_date = datetime.today() - timedelta(days=7)
    max_date = datetime.today()

    if filter_option == 'By Date Filter':
        start = st.date_input("Start Date", min_date)
        end = st.date_input("End Date", max_date)
        date_range = (start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
    elif filter_option == 'Today':
        today = datetime.today()
        date_range = (
            today.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S'),
            today.replace(hour=23, minute=59, second=59).strftime('%Y-%m-%d %H:%M:%S')
        )
    elif filter_option == 'Last 24 Hours':
        now = datetime.now()
        date_range = (
            (now - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S'),
            now.strftime('%Y-%m-%d %H:%M:%S')
        )
    elif filter_option == 'Last 7 Days':
        now = datetime.now()
        date_range = (
            (now - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S'),
            now.strftime('%Y-%m-%d %H:%M:%S')
        )

    # --- Data Loading ---
    def get_connection():
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=20.166.7.4,5003;"
            "DATABASE=Simplitics;"
            "UID=LiaUser;"
            "PWD=LuciaToday!"
        )
        return pyodbc.connect(conn_str)

    @st.cache_data
    def load_data(date_range=None):
        query = """
            SELECT SrcFileAbbr, SrcFileKey, SrcRealFileNm, ExtractnTm, FileWriteTm, LoadTm, ParseExecTm, SrcFileType, MD5, Filesize, BatchInstId
            FROM [Simplitics].[audit].[ADTSrcFile]
        """
        if date_range:
            start_date, end_date = date_range
            query += f" WHERE LoadTm >= '{start_date}' AND LoadTm <= '{end_date}'"

        with get_connection() as conn:
            df = pd.read_sql(query, conn)

        for col in ['ExtractnTm', 'FileWriteTm', 'LoadTm', 'ParseExecTm']:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        return df

    df = load_data(date_range)

    if df.empty:
        st.warning("⚠️ No data available for selected date range.")
        return

    # --- Filters ---
    st.subheader("🔍 Filter by Source File Abbr")
    files = df['SrcFileAbbr'].dropna().unique().tolist()
    files.insert(0, "All")
    selected_file = st.selectbox("Select File", files)

    if selected_file != "All":
        df = df[df['SrcFileAbbr'] == selected_file]

    if df.empty:
        st.warning("⚠️ No data available for selected filters.")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("📦 Total Files", f"{len(df)}")
    col2.metric("🔤 Unique Sources", f"{df['SrcFileAbbr'].nunique()}")
    col3.metric("🕒 Last File Loaded", df['LoadTm'].max().strftime("%Y-%m-%d"))

    df['EventDate'] = df['LoadTm'].dt.date
    files_per_day = df.groupby('EventDate').size().reset_index(name='FileCount')
    files_per_day['RollingAvg'] = files_per_day['FileCount'].rolling(7, min_periods=1).mean()

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(px.bar(files_per_day, x='EventDate', y='FileCount', title="Files Loaded Per Day"), use_container_width=True)
    with col2:
        st.plotly_chart(px.line(files_per_day, x='EventDate', y='RollingAvg', title="7-Day Rolling Avg of File Loads"), use_container_width=True)

    df['ProcessingTimeSec'] = (df['LoadTm'] - df['FileWriteTm']).dt.total_seconds()
    df = df[df['ProcessingTimeSec'] >= 0]

    if not df.empty:
        st.subheader("⏱️ Load Processing Time")
        col1, col2, col3 = st.columns(3)
        col1.metric("Average", f"{df['ProcessingTimeSec'].mean():.2f} sec")
        col2.metric("Min", f"{df['ProcessingTimeSec'].min():.2f} sec")
        col3.metric("Max", f"{df['ProcessingTimeSec'].max():.2f} sec")

        st.plotly_chart(
            px.bar(df, x='SrcFileKey', y='ProcessingTimeSec', title="Processing Time Per File", color='ProcessingTimeSec'),
            use_container_width=True
        )

    df['Hour'] = df['LoadTm'].dt.hour
    df['Day'] = df['LoadTm'].dt.day_name()
    heatmap_df = df.groupby(['Day', 'Hour']).size().reset_index(name='File Count')
    heatmap_pivot = heatmap_df.pivot(index='Day', columns='Hour', values='File Count').fillna(0)

    fig = px.imshow(heatmap_pivot, aspect='auto', color_continuous_scale='Viridis',
                    title="📅 File Arrivals Heatmap (Day vs Hour)")
    st.plotly_chart(fig, use_container_width=True)

