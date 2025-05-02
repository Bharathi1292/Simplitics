import streamlit as st
import pandas as pd
import pyodbc
import plotly.express as px
from datetime import datetime, timedelta

def get_data():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=20.166.7.4,5003;'
        'DATABASE=Simplitics;'
        'UID=LiaUser;PWD=LuciaToday!'
    )
    query = """
    SELECT SrcFileAbbr, LdSchedDttm, CurrLdStp, LdStpDttm, 
           LdStpStartDttm, LdStpEndDttm, LdStpStatus, 
           LdStpExcptTxt, LdStpExcptTraceTxt, LoadTm
    FROM Simplitics.dbo.MDLdSchedStps
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def transform_data(df):
    df.rename(columns={
        "SrcFileAbbr": "SourceFile",
        "LdSchedDttm": "ScheduledTime",
        "LdStpDttm": "StepTime",
        "LdStpStartDttm": "StepStartTime",
        "LdStpEndDttm": "StepEndTime",
        "LdStpStatus": "StepStatus",
        "LdStpExcptTxt": "ExceptionText",
        "LdStpExcptTraceTxt": "ExceptionTrace",
        "LoadTm": "LoadTime"
    }, inplace=True)

    datetime_cols = ["ScheduledTime", "StepTime", "StepStartTime", "StepEndTime", "LoadTime"]
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    status_map = {
        0: "Initiated/Queued",
        1000: "Running",
        2000: "Successfully Executed",
        9999: "Failure"
    }
    df["StepStatus"] = df["StepStatus"].map(status_map)

    df["Actual_Duration"] = (df["StepEndTime"] - df["StepStartTime"]).dt.total_seconds()
    df["Total_Duration"] = df.groupby(["SourceFile", "ScheduledTime"])["Actual_Duration"].transform("sum")

    return df

def show():
    
    st.title("MDLdSchedStps Analysis")

    df = transform_data(get_data())

    col1, col2 = st.columns(2)
    with col1:
        view_option = st.radio(
            "Select View",
            ["Overview", "Execution Times", "Distributions & Trends"],
            horizontal=True
        )

    # --- Filter Section ---
    st.header("🔍 Filter Data")
    st.subheader("🗓️ Date Filters")
    filter_option = st.radio("Select a period", ['By Date Filter', 'Today', 'Last 7 Days'], horizontal=True)

    date_range = None
    min_date = df["ScheduledTime"].min().date()
    max_date = df["ScheduledTime"].max().date()
    today_date = datetime.today().date()

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
    elif filter_option == 'Last 7 Days':
        now = datetime.now()
        date_range = (
            (now - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S'),
            now.strftime('%Y-%m-%d %H:%M:%S')
        )

    with st.expander("📁 Filter by Source File"):
        source_files = st.multiselect(
            "Select Source File(s)",
            options=df["SourceFile"].unique(),
            default=df["SourceFile"].unique()
        )

    with st.expander("📌 Filter by Step Status"):
        step_statuses = st.multiselect(
            "Select Step Status",
            options=df["StepStatus"].dropna().unique(),
            default=df["StepStatus"].dropna().unique()
        )

    filtered_df = df[
        (df["ScheduledTime"].dt.strftime('%Y-%m-%d %H:%M:%S') >= date_range[0]) &
        (df["ScheduledTime"].dt.strftime('%Y-%m-%d %H:%M:%S') <= date_range[1]) &
        (df["SourceFile"].isin(source_files)) &
        (df["StepStatus"].isin(step_statuses))
    ]

    if not filtered_df.empty:
        st.write(f"Filtered Data from {date_range[0]} to {date_range[1]}")
        st.dataframe(filtered_df)
    else:
        st.write("No data available for the selected filters.")

    # =============== OVERVIEW ===============
    if view_option == "Overview":
        st.subheader("🗂 Filtered Records")
        st.dataframe(filtered_df)

        status_counts = df["StepStatus"].value_counts().reset_index()
        status_counts.columns = ["StepStatus", "Count"]
        fig_pie = px.pie(status_counts, names="StepStatus", values="Count", title="📊 Overall Step Status Distribution")
        st.plotly_chart(fig_pie)

        failure_counts = df[df["StepStatus"] == "Failure"].groupby("SourceFile").size().reset_index(name="Failures")
        fig_fail = px.bar(failure_counts, x="Failures", y="SourceFile", orientation="h",
                          title="❌ Number of Failures per Source File", color="Failures")
        st.plotly_chart(fig_fail)

    # =============== EXECUTION TIMES ===============
    elif view_option == "Execution Times":
        st.subheader("📈 Average Actual Duration by Source File")
        avg_duration = filtered_df.groupby("SourceFile")["Actual_Duration"].mean().reset_index()
        st.bar_chart(avg_duration.set_index("SourceFile"))

        df_summary = filtered_df.groupby(["SourceFile", "ScheduledTime"], as_index=False).agg(
            Number_of_Steps=("SourceFile", "count"),
            Total_Execution_Time_Sec=("Actual_Duration", "sum"),
            Avg_Duration_Per_Step=("Actual_Duration", "mean"),
            Min_Start_Time=("StepStartTime", "min"),
            Max_End_Time=("StepEndTime", "max")
        )

        df_summary["Actual_Calendar_Time_Sec"] = (
            df_summary["Max_End_Time"] - df_summary["Min_Start_Time"]
        ).dt.total_seconds()

        df_summary["Work_ID"] = df_summary["SourceFile"] + "_" + df_summary["ScheduledTime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df_summary.drop(columns=["Min_Start_Time", "Max_End_Time"], inplace=True)

        status_counts = filtered_df.groupby(["SourceFile", "ScheduledTime"])["StepStatus"].nunique().reset_index()
        status_counts["Is_Complete"] = status_counts["StepStatus"] == 1
        df_summary = df_summary.merge(status_counts[["SourceFile", "ScheduledTime", "Is_Complete"]],
                                      on=["SourceFile", "ScheduledTime"], how="left")

        st.title("⏳ Execution Time Analysis")
        view_level = st.radio("View Level:", ["Per Source File", "Per Work"], horizontal=True)
        view_metric = st.radio("Select Metric:", ["Total Execution Time", "Actual Calendar Time"], horizontal=True)

        if view_metric == "Total Execution Time":
            y_column = "Total_Execution_Time_Sec"
            chart_title = "Total Execution Time"
        else:
            y_column = "Actual_Calendar_Time_Sec"
            chart_title = "Actual Calendar Time"

        if view_level == "Per Work":
            fig = px.bar(df_summary, x="Work_ID", y=y_column, text=y_column,
                         color=y_column, title=f"{chart_title} per Work",
                         color_continuous_scale="Blues")
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig)
        else:
            df_agg = df_summary.groupby("SourceFile", as_index=False).agg({
                "Total_Execution_Time_Sec": "sum",
                "Actual_Calendar_Time_Sec": "sum"
            })

            fig_agg = px.bar(df_agg, x="SourceFile", y=y_column, text=y_column,
                             color=y_column, title=f"{chart_title} per Source File (Aggregated)",
                             color_continuous_scale="Purples")
            fig_agg.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_agg)

        st.subheader("📋 Execution Summary Table")
        st.dataframe(df_summary)

        df_trend = df.groupby(df["ScheduledTime"].dt.date)["Actual_Duration"].sum().reset_index()
        df_trend.columns = ["ScheduledDate", "Total_Execution_Time"]
        fig_trend = px.line(df_trend, x="ScheduledDate", y="Total_Execution_Time",
                            markers=True, title="📈 Daily Total Execution Time Trend")
        st.plotly_chart(fig_trend)

        df_trend["7_day_avg"] = df_trend["Total_Execution_Time"].rolling(window=7).mean()
        df_trend["30_day_avg"] = df_trend["Total_Execution_Time"].rolling(window=30).mean()
        fig_roll = px.line(df_trend, x="ScheduledDate", y=["Total_Execution_Time", "7_day_avg", "30_day_avg"],
                           labels={"value": "Execution Time", "ScheduledDate": "Date", "variable": "Metric"},
                           title="📉 Execution Time with Rolling Averages")
        st.plotly_chart(fig_roll)

    # =============== DISTRIBUTIONS & TRENDS ===============
    elif view_option == "Distributions & Trends":
        st.title("🐢 Top Long-Running Tasks")
        top_n = st.slider("Select number of top long-running steps to show:", min_value=5, max_value=20, value=10)
        df_top_tasks = df.sort_values(by="Actual_Duration", ascending=False).head(top_n)

        fig_top = px.bar(df_top_tasks, x="Actual_Duration", y="StepTime", color="SourceFile",
                         orientation="h", text="Actual_Duration",
                         title=f"Top {top_n} Long-Running Steps",
                         labels={"Actual_Duration": "Duration (seconds)", "StepTime": "Step Time"})
        st.plotly_chart(fig_top, use_container_width=True)

        st.subheader(f"📋 Top {top_n} Step Details")
        st.dataframe(df_top_tasks)

        df_avg_duration = df.groupby("SourceFile", as_index=False).agg(
            Total_Duration=("Actual_Duration", "sum"),
            Step_Count=("StepTime", "count")
        )
        df_avg_duration["Avg_Duration"] = df_avg_duration["Total_Duration"] / df_avg_duration["Step_Count"]

        fig3 = px.bar(df_avg_duration, x="Avg_Duration", y="SourceFile", orientation="h",
                      text="Avg_Duration", title="Average Execution Time per Source File (Based on Steps)",
                      color="Avg_Duration", color_continuous_scale="Viridis")
        st.plotly_chart(fig3)

        fig4 = px.box(df, x="SourceFile", y="Actual_Duration", color="SourceFile",
                      title="Box Plot: Execution Time Distribution")
        st.plotly_chart(fig4)

        df["Hour"] = df["StepStartTime"].dt.hour
        df["Weekday"] = df["StepStartTime"].dt.day_name()
        activity = df.groupby(["Weekday", "Hour"]).size().reset_index(name="Count")

        weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        activity["Weekday"] = pd.Categorical(activity["Weekday"], categories=weekday_order, ordered=True)

        fig_heat = px.density_heatmap(activity, x="Hour", y="Weekday", z="Count",
                                      title="🔥 Heatmap of Step Activity by Hour and Day",
                                      color_continuous_scale="Inferno")
        st.plotly_chart(fig_heat)
