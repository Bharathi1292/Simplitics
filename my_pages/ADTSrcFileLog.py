import streamlit as st
import pandas as pd
import plotly.express as px
import pyodbc
from datetime import datetime, timedelta

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

@st.cache_data
def load_data_from_sql(date_range=None):
    query = """
        SELECT SrcFileAbbr, SrcFileKey, eventTime, landingReadyAt, trustedReadyAt, LoadTm, rawReadyAt, profileReadyAt, LastSeenAt
        FROM [Simplitics].[audit].[ADTSrcFileLog]
    """
    filters = []
    if date_range:
        start_date, end_date = date_range
        filters.append(f"eventTime >= '{start_date}' AND eventTime <= '{end_date}'")
    if filters:
        query += " WHERE " + " AND ".join(filters)

    with get_connection() as conn:
        df = pd.read_sql(query, conn)

    for col in ['eventTime', 'landingReadyAt', 'trustedReadyAt', 'rawReadyAt', 'profileReadyAt']:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    return df

def show():
    st.title("📁 ADTSrcFileLog Analysis")

    # Date Filter Section
    st.subheader("🔍 Date Filters")
    filter_option = st.radio("Select a period", ['By Date Filter', 'Today', 'Last 24 Hours', 'Last 7 Days'], index=0, horizontal=True,key="date_filter_radio")
    date_range = None
    col1, col2 = st.columns(2)

    if filter_option == 'By Date Filter':
        with col1:
            start_date = st.date_input("Start Date", value=datetime.today().date() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End Date", value=datetime.today().date())
        date_range = (start_date, end_date)
    elif filter_option == 'Today':
        today = datetime.today().date()
        date_range = (datetime.combine(today, datetime.min.time()), datetime.combine(today, datetime.max.time()))
    elif filter_option == 'Last 24 Hours':
        date_range = (datetime.now() - timedelta(hours=24), datetime.now())
    elif filter_option == 'Last 7 Days':
        date_range = (datetime.now() - timedelta(days=7), datetime.now())

    # Source File Filter
    st.subheader("🔍 Source File Filter")
    df_for_filter = load_data_from_sql(date_range)
    available_files = ["All"] + sorted(df_for_filter['SrcFileAbbr'].dropna().unique())
    selected_source_file = st.selectbox("Source Filter", available_files)

    df = df_for_filter.copy()
    if selected_source_file != "All":
        df = df[df['SrcFileAbbr'] == selected_source_file]

    if df.empty:
        st.warning("⚠️ No data available for the selected filters.")
        return

    # Processing Time Calculation
    df['ProcessingTime'] = (df['trustedReadyAt'] - df['landingReadyAt']).dt.total_seconds().clip(lower=0)

    # File Status Metrics
    landing_files = df['landingReadyAt'].notna().sum()
    raw_files = df['rawReadyAt'].notna().sum()
    trusted_files = df['trustedReadyAt'].notna().sum()
    staged_files = df['LastSeenAt'].isna().sum()
    profile_files = df['profileReadyAt'].notna().sum()

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Landing", landing_files)
    col2.metric("Raw", raw_files)
    col3.metric("Trusted", trusted_files)
    col4.metric("Stage", staged_files)
    col5.metric("Profile", profile_files)

    st.dataframe(df)

    # Files per Day Chart
    df['EventDate'] = df['eventTime'].dt.date
    files_per_day = df.groupby('EventDate').size().reset_index(name='FilesArrived')
    files_per_day['RollingAvg'] = files_per_day['FilesArrived'].rolling(window=7, min_periods=1).mean()

    fig1 = px.bar(files_per_day, x='EventDate', y='FilesArrived', title="📊 Files Arrived Per Date", color='FilesArrived', color_continuous_scale='Viridis')
    fig2 = px.line(files_per_day, x='EventDate', y='RollingAvg', title="📈 7-Day Rolling Average", markers=True)
    fig3 = px.bar(files_per_day, x='EventDate', y='FilesArrived', title="📊 Files vs Rolling Avg", color='FilesArrived', color_continuous_scale='Viridis')
    fig3.add_scatter(x=files_per_day['EventDate'], y=files_per_day['RollingAvg'], mode='lines+markers', name='Rolling Avg', line=dict(color='red', width=2))

    st.title("📅 File Arrival Analysis")
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig1, use_container_width=True)
    with col2:
        st.plotly_chart(fig2, use_container_width=True)
    st.plotly_chart(fig3, use_container_width=True)

    # Processing Time Stats
    df_time = df.dropna(subset=['landingReadyAt', 'trustedReadyAt']).copy()
    df_time['processing_time_seconds'] = (df_time['trustedReadyAt'] - df_time['landingReadyAt']).dt.total_seconds()
    df_time = df_time[df_time['processing_time_seconds'] >= 0]

    if df_time.empty:
        st.warning("⚠️ No valid processing time data available.")
        return

    avg_time = df_time['processing_time_seconds'].mean()
    min_time = df_time['processing_time_seconds'].min()
    max_time = df_time['processing_time_seconds'].max()

    st.title("⏱️ Processing Time Analysis")
    col1, col2, col3 = st.columns(3)
    col1.metric("Average", f"{avg_time:.2f} sec")
    col2.metric("Min", f"{min_time:.2f} sec")
    col3.metric("Max", f"{max_time:.2f} sec")

    fig_bar = px.bar(df_time, x='SrcFileKey', y='processing_time_seconds', title="📊 Processing Time Per File", color='processing_time_seconds', color_continuous_scale='reds')
    fig_bar.update_layout(height=500, xaxis_tickangle=-45, margin=dict(l=40, r=40, t=60, b=150))
    st.plotly_chart(fig_bar, use_container_width=True)

    df_time['LandingHour'] = df_time['landingReadyAt'].dt.hour
    fig = px.scatter(df_time, x='LandingHour', y='SrcFileAbbr', size='ProcessingTime', color='SrcFileAbbr',
                     title="📦 File Deliveries by Hour",
                     hover_data=['SrcFileKey', 'landingReadyAt', 'trustedReadyAt', 'ProcessingTime'],
                     category_orders={'LandingHour': list(range(0, 24))})
    fig.update_layout(xaxis=dict(tickmode='linear', tick0=0, dtick=1))
    st.subheader("📦 File Deliveries per 24 Hours")
    st.plotly_chart(fig, use_container_width=True)

