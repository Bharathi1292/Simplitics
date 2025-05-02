import streamlit as st
import pandas as pd
import plotly.express as px
import pyodbc
from datetime import datetime, timedelta

# SQL connection function
def get_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=20.166.7.4,5003;"
        "DATABASE=Simplitics;"
        "UID=LiaUser;"
        "PWD=LuciaToday!"
    )
    return pyodbc.connect(conn_str)

# Data loading
@st.cache_data
# Data loading function
@st.cache_data
def load_mdl_schedstats_data(start_date, end_date):
    # Convert dates to string format for SQL query
    start_date_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, datetime) else str(start_date)
    end_date_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, datetime) else str(end_date)

    query = f"""
        SELECT ExecutionDttm, ObjNm, SrcFileAbbr, AttrNm, StagedCnt, TgtInsertCnt, LoadTm
        FROM MDLdSchedStats
        WHERE ExecutionDttm BETWEEN '{start_date_str}' AND '{end_date_str}'
    """

    with get_connection() as conn:
        df = pd.read_sql(query, conn)

    df["ExecutionDttm"] = pd.to_datetime(df["ExecutionDttm"])

    return df


# Main function that contains the page views
def show():
    # Main page navigation
    st.title("MDLdSchedStats Analysis")
    page = st.radio("Select View", ["📁 Source File Perspective", "📁 Model Perspective"], horizontal=True)

    today = datetime.today().date()

    # --- Source File Perspective ---
    if page == "📁 Source File Perspective":
        # Filter: Date Options
        st.subheader("🔍 Date Filters")
        filter_option = st.radio("Select a period", ['By Date Filter', 'Today', 'Last 24 Hours', 'Last 7 Days'], index=0, horizontal=True)

        start_date, end_date = today, today
        if filter_option == 'By Date Filter':
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", value=today - timedelta(days=7))
            with col2:
                end_date = st.date_input("End Date", value=today)
        elif filter_option == 'Today':
            start_date, end_date = today, today
        elif filter_option == 'Last 24 Hours':
            start_date = datetime.now() - timedelta(hours=24)
            end_date = datetime.now()
        elif filter_option == 'Last 7 Days':
            start_date = datetime.now() - timedelta(days=7)
            end_date = datetime.now()

        # Load data
        df = load_mdl_schedstats_data(start_date, end_date)

        # Filter: Source File
        st.subheader("🔍 Source File Filter")
        src_file_options = df['SrcFileAbbr'].dropna().unique().tolist()
        src_file_options.insert(0, "All")
        selected_src_file = st.selectbox("Source File", src_file_options)

        if selected_src_file != "All":
            df = df[df['SrcFileAbbr'] == selected_src_file]

        if df.empty:
            st.warning("⚠️ No data available for the selected filters.")
        else:
            # Metrics
            st.subheader("📊 Metrics Overview")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Source Files", df['SrcFileAbbr'].nunique())
            col2.metric("Objects", df['ObjNm'].nunique())
            col3.metric("Attributes", df['AttrNm'].nunique())
            col4.metric("Total Records", len(df))

            # Bar Charts
            st.subheader("📈 Visualizations")
            min_max_staged = df.groupby(['ExecutionDttm', 'SrcFileAbbr']).agg(
                Min_Staged=('StagedCnt', 'min'),
                Max_Staged=('StagedCnt', 'max')
            ).reset_index()

            fig1 = px.bar(min_max_staged, x="ExecutionDttm", y=["Min_Staged", "Max_Staged"], 
                          color="SrcFileAbbr", barmode="group", title="Min and Max Staged Count per File")
            st.plotly_chart(fig1, use_container_width=True)

            df['ExecutionDttm'] = pd.to_datetime(df['ExecutionDttm'])
            df['LoadTm'] = pd.to_datetime(df['LoadTm'])

            # Processing time calculation
            df['processing_time_seconds'] = (df['LoadTm'] - df['ExecutionDttm']).dt.total_seconds()

            fig2 = px.histogram(df, x='processing_time_seconds', nbins=30,
                                 title="⏳ Distribution of Processing Times")
            st.plotly_chart(fig2, use_container_width=True)

            # Rolling average for staged count
            roll_avg_staged = df.groupby(['ExecutionDttm', 'SrcFileAbbr'])['StagedCnt'].sum().reset_index()
            roll_avg_staged["RollingAvg_StagedCnt"] = roll_avg_staged.groupby("SrcFileAbbr")["StagedCnt"].transform(
                lambda x: x.rolling(window=7, min_periods=1).mean()
            )

            fig3 = px.line(roll_avg_staged, x="ExecutionDttm", y="RollingAvg_StagedCnt", color="SrcFileAbbr",
                           title="7-Day Rolling Avg of Staged Count")
            st.plotly_chart(fig3, use_container_width=True)

            avg_processing_by_src = df.groupby('SrcFileAbbr')['processing_time_seconds'].mean().sort_values(ascending=False).head(10).reset_index()

            fig4 = px.bar(avg_processing_by_src, x='SrcFileAbbr', y='processing_time_seconds',
                          title="🐢 Slowest Source Files (Top 10)", labels={'processing_time_seconds': 'Avg Processing Time (sec)'})
            st.plotly_chart(fig4, use_container_width=True)

    # --- Model Perspective ---
    if page == "📁 Model Perspective":
        # Filter: Date Options (same logic as above)
        st.subheader("🔍 Date Filters")
        filter_option = st.radio("Select a period", ['By Date Filter', 'Today', 'Last 24 Hours', 'Last 7 Days'], index=0, horizontal=True)

        start_date, end_date = today, today
        if filter_option == 'By Date Filter':
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", value=today - timedelta(days=7))
            with col2:
                end_date = st.date_input("End Date", value=today)
        elif filter_option == 'Today':
            start_date, end_date = today, today
        elif filter_option == 'Last 24 Hours':
            start_date = datetime.now() - timedelta(hours=24)
            end_date = datetime.now()
        elif filter_option == 'Last 7 Days':
            start_date = datetime.now() - timedelta(days=7)
            end_date = datetime.now()

        # Load data
        df = load_mdl_schedstats_data(start_date, end_date)

        if df.empty:
            st.warning("⚠️ No data available for the selected filters.")
        else:
            # Metrics
            st.subheader("📊 Metrics Overview")
            col1, col2, col3 = st.columns(3)
            col1.metric("Objects", df['ObjNm'].nunique())
            col2.metric("Source Files", df['SrcFileAbbr'].nunique())
            col3.metric("Attributes", df['AttrNm'].nunique())

            # Bar Charts
            st.subheader("📈 Visualizations")
            obj_sourcefile_count = df.groupby(["ExecutionDttm", "ObjNm"])["SrcFileAbbr"].nunique().reset_index()
            fig3 = px.bar(obj_sourcefile_count, x="ExecutionDttm", y="SrcFileAbbr", color="ObjNm",
                          title="Source Files Loaded per Object", labels={"SrcFileAbbr": "Source Files"})

            obj_attr_count = df.groupby(["ExecutionDttm", "ObjNm"])["AttrNm"].nunique().reset_index()
            fig4 = px.bar(obj_attr_count, x="ExecutionDttm", y="AttrNm", color="ObjNm",
                          title="Attributes Loaded per Object", labels={"AttrNm": "Attributes"})

            rolling_insert = df.groupby(["ExecutionDttm", "ObjNm"])["TgtInsertCnt"].sum().reset_index()
            rolling_insert["RollingAvg_InsertCnt"] = rolling_insert.groupby("ObjNm")["TgtInsertCnt"].transform(
                lambda x: x.rolling(7, min_periods=1).mean()
            )
            fig5 = px.line(rolling_insert, x="ExecutionDttm", y="RollingAvg_InsertCnt", color="ObjNm",
                           title="7-Day Rolling Avg Insert Count per Object")

            col1, col2 = st.columns(2)
            with col1:
                st.plotly_chart(fig3, use_container_width=True)
            with col2:
                st.plotly_chart(fig4, use_container_width=True)

            st.plotly_chart(fig5, use_container_width=True)

