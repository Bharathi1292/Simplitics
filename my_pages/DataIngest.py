import pandas as pd
import pyodbc
import streamlit as st
from datetime import datetime, timedelta
import plotly.express as px

# SQL Connection
def get_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=20.166.7.4,5003;"
        "DATABASE=Simplitics;"
        "UID=LiaUser;"
        "PWD=LuciaToday!"
    )


@st.cache_data(ttl=3600)
def fetch_and_merge_data():
    conn = get_connection()
    tables = {
        "MDSrcFileExportStps": "MDSrcFileExportStps",
        "MDSrcApiExportStps": "MDSrcApiExportStps",
        "MDSrcCustomExportStps": "MDSrcCustomExportStps",
        "MDSrcTblExportStps": "MDSrcTblExportStps"
    }

    dfs = []
    for name, table in tables.items():
        query = f"""
            SELECT 
                ExportStpStartDttm AS ExportStepStartTime,
                ExportStpEndDttm AS ExportStepEndTime,
                ExportStpStatus AS ExportStepStatus,
                DATEDIFF(SECOND, ExportStpStartDttm, ExportStpEndDttm) AS Execution_Time,
                '{name}' AS SourceTableName
            FROM {table}
            WHERE ExportStpStartDttm >= DATEADD(DAY, -90, GETDATE())
        """
        df = pd.read_sql(query, conn)
        dfs.append(df)

    conn.close()

    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df["ExportStepStartTime"] = pd.to_datetime(combined_df["ExportStepStartTime"], errors="coerce")
    combined_df["ExportStepEndTime"] = pd.to_datetime(combined_df["ExportStepEndTime"], errors="coerce")

    return combined_df


# 🔍 Filtering Data Based on User Selection
def filter_data(df, filter_type, min_date, max_date, start_date=None, end_date=None):
    """Filter data based on user selection."""
    if filter_type == "Last 7 Days":
        cutoff_date = max_date - timedelta(days=7)
    elif filter_type == "Last 30 Days":
        cutoff_date = max_date - timedelta(days=30)
    elif filter_type == "Last 90 Days":
        cutoff_date = max_date - timedelta(days=90)
    elif filter_type == "Custom Date Range" and start_date and end_date:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        return df[(df["ExportStepStartTime"] >= start_date) & (df["ExportStepStartTime"] <= end_date)]
    else:
        return df  # Default return full dataset

    return df[df["ExportStepStartTime"] >= cutoff_date]


# ⏳ Computing Execution Time Trends with Rolling Averages
def compute_trends(df, window_size):
    """Compute execution time trends with rolling averages."""
    df = df.copy()
    df["Date"] = df["ExportStepStartTime"].dt.date
    daily_avg = df.groupby("Date")["Execution_Time"].mean().reset_index()
    daily_avg["Execution_Time"] = daily_avg["Execution_Time"].astype(float)  # ensure float
    daily_avg[f"{window_size}-Day Avg"] = daily_avg["Execution_Time"].rolling(window=window_size, min_periods=1).mean().astype(float)
    return daily_avg


# Main page navigation 
def show():
    st.title("Data Ingest ")
    page = st.radio("Go to", ["📊 Overview", "📁 Data Analysis", "⚡ Data Efficiency Over Time"], horizontal=True)
    st.markdown("---")  # Optional: adds a divider line
    
    # Load the data
    df = fetch_and_merge_data()

    status_mapping = {
        0: "initiated/queued",
        1000: "running",
        2000: "successfully executed",
        9999: "failure"
    }

    df["ExportStepStatus"] = df["ExportStepStatus"].map(status_mapping)

    # ----------------Overview  ----------------
    if page == "📊 Overview":
        st.title("📊 Overview - Data Ingest Dashboard")
        st.markdown("Welcome to the Source File Processing Dashboard! Use the *radio button* to navigate between pages.")
        
        st.subheader("📌 What are we monitoring?")
        st.markdown("""
        - 🚀 **Data Efficiency**: Ensure we efficiently copy and query source data.
        - ⏳ **Execution Time Trends**: Monitor task durations and detect anomalies.
        - 🛠 **System Health**: Identify performance issues before they escalate.
        """)

        st.header("📊 Data Overview")
        st.subheader("📥 Load and Preprocess Data")

        min_date = df["ExportStepStartTime"].min()
        max_date = df["ExportStepStartTime"].max()

        st.success("✅ Data loaded successfully!")

        with st.expander("📅 Unique Days", expanded=False):
            st.write(f"Number of unique days: `{df['ExportStepStartTime'].dt.date.nunique()}`")

        with st.expander("📊 Dataset Shape", expanded=False):
            st.write(f"Number of columns: `{df.shape[1]}`")
            st.write(f"Number of rows: `{df.shape[0]}`")

        with st.expander("📆 Data Time Range", expanded=False):
            st.write(f"Minimum Date: `{min_date}`")
            st.write(f"Maximum Date: `{max_date}`")

        with st.expander("📜 Records Older Than 90 Days", expanded=False):
            older_data = df[df["ExportStepStartTime"] < pd.Timestamp.now() - pd.DateOffset(days=90)]
            st.write(f"Records older than 90 days: `{len(older_data)}`")

        with st.expander("📌 Filter Data", expanded=False):
            status_options = ["All"] + sorted(df["ExportStepStatus"].dropna().unique().tolist())
            selected_status = st.selectbox("Filter by Status:", status_options)

            table_options = ["All"] + sorted(df["SourceTableName"].unique())
            selected_table = st.selectbox("Filter by Source Table:", table_options)

            filtered_df = df.copy()
            if selected_status != "All":
                filtered_df = filtered_df[filtered_df["ExportStepStatus"] == selected_status]
            if selected_table != "All":
                filtered_df = filtered_df[filtered_df["SourceTableName"] == selected_table]

            st.dataframe(filtered_df, use_container_width=True)

        with st.expander("📅 Unique Dates in Data", expanded=False):
            st.write("Unique Dates:", df["ExportStepStartTime"].dt.date.unique())

    # ---------------- DATA ANALYSIS ----------------
    elif page == "📁 Data Analysis":
        st.title("📁 Data Analysis")
        analysis_type = st.radio("What do you want to analyze?", ["Execution Trends", "System Health"], horizontal=True)

        if analysis_type == "Execution Trends":
            st.subheader("🔍 Rolling Average of Execution Duration")
            filter_type = st.radio("Select Date Range:", ["Custom Date Range", "Last 7 Days", "Last 30 Days", "Last 90 Days"], horizontal=True)
            rolling_window = {"Last 7 Days": 7, "Last 30 Days": 30, "Last 90 Days": 90}.get(filter_type, None)
            df_filtered = filter_data(df, filter_type, df["ExportStepStartTime"].min(), df["ExportStepStartTime"].max())

            if not df_filtered.empty:
                trend_df = compute_trends(df_filtered, rolling_window or 7)
                y_columns = ["Execution_Time"] if rolling_window is None else ["Execution_Time", f"{rolling_window}-Day Avg"]

                fig = px.line(
                    trend_df, x="Date", y=y_columns,
                    labels={"value": "Execution Time (seconds)", "variable": "Metric"},
                    title=f"Execution Time Trends - {filter_type}",
                    markers=True
                )
                st.plotly_chart(fig)

        elif analysis_type == "System Health":
            st.subheader("📈 System Health Overview")
            trend_df = compute_trends(df, window_size=7)
            st.dataframe(trend_df)

    # ---------------- DATA EFFICIENCY OVER TIME ----------------
    elif page == "⚡ Data Efficiency Over Time":
        st.title("⚡ Data Efficiency Over Time")
        df_eff = df.dropna(subset=["ExportStepStartTime"]).copy()
        df_eff["Date"] = df_eff["ExportStepStartTime"].dt.date

        status_counts = df_eff["ExportStepStatus"].value_counts().reset_index()
        status_counts.columns = ["ExportStepStatus", "Count"]
        st.dataframe(status_counts)

        efficiency_df = df_eff.groupby(["Date", "ExportStepStatus"]).size().reset_index(name="Count")

        status_colors = {
            "successfully executed": "#2ECC71",  # Green
            "failure": "#E74C3C",                # Red
            "running": "#F39C12",                # Orange
            "initiated/queued": "#9B59B6"        # Purple
        }

        if efficiency_df.empty:
            st.warning("⚠️ No export data available for efficiency analysis.")
        else:
            st.subheader("📊 Daily Export Step Efficiency Trends")
            fig_eff = px.bar(
                efficiency_df, x="Date", y="Count", color="ExportStepStatus",
                title="Export Step Status Over Time (Stacked)",
                labels={"Count": "Number of Steps", "ExportStepStatus": "Status"},
                color_discrete_map=status_colors,
                barmode="stack"
            )
            st.plotly_chart(fig_eff, use_container_width=True)



