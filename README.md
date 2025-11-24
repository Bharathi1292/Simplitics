# Simplitics LIA Dashboard

![Streamlit](https://img.shields.io/badge/Framework-Streamlit-red)
![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Status](https://img.shields.io/badge/Version-1.0-success)
![License](https://img.shields.io/badge/License-Proprietary-lightgrey)

A Streamlit-based monitoring tool for visualizing and analyzing the **Simplitics LIA Data Pipeline**, providing real-time operational insights into job execution, scheduling, ingestion pipelines, and file processing.

---

## 🏠 Overview

The dashboard centralizes operational metrics and helps users:

* Track job execution statuses
* Inspect detailed logs
* Review file processing history
* Evaluate ingestion performance
* Observe scheduling behavior and trends

Navigation uses Streamlit's `query_params` for state-based page routing, giving the app a smooth multi-page experience.

---

## 🚀 Features

### 🔧 System Dashboards

* **📁 ADTSrcFile** – Browse incoming source file activity
* **📝 ADTSrcFileLog** – Inspect processing logs in detail
* **🔍 MDJobTracer** – Trace job execution paths and timelines
* **📓 MDJobLogger** – Explore job results, errors, stack traces
* **📈 MDLdSchedStats** – View loading volumes and aggregation metrics
* **📊 MDLdSchedStps** – Analyze detailed load steps
* **🚚 DataIngest** – Monitor ingestion timing and pipeline throughput

### 🎨 UI Experience

* Gradient background home dashboard
* Tile-style navigation buttons
* Sidebar with Back-to-Home functionality
* Responsive layout using Streamlit’s column grid

---

## 📂 Project Structure

```
project/
│
├── main.py
├── my_pages/
│   ├── ADTSrcFile.py
│   ├── ADTSrcFileLog.py
│   ├── MDJobTracer.py
│   ├── MDJobLogger.py
│   ├── MDLdSchedStats.py
│   ├── MDLdSchedStps.py
│   └── DataIngest.py
└── README.md
```

Each module exposes a `show()` method that Streamlit calls when the page is active.

---

## ▶️ Installation & Run

### 1️⃣ Install dependencies

```
pip install streamlit
```

(Install additional libraries if your modules require them.)

### 2️⃣ Launch the dashboard

```
streamlit run main.py
```

---

## 🖼 Screenshots
1. •	File Arrival Analysis – Shows the timing and volume of incoming data files. 

<img width="868" height="377" alt="image" src="https://github.com/user-attachments/assets/a313861c-e4f7-4078-815d-500c807ad9c9" />

2.•	Job Count Over Time – Displays daily job execution trends. 

<img width="872" height="376" alt="image" src="https://github.com/user-attachments/assets/fb3a8e17-eb2a-49c2-9096-0ecb92582637" />

3.•	Hourly Job Distribution – Illustrates peak hours of job activity to optimize scheduling. 

<img width="879" height="374" alt="image" src="https://github.com/user-attachments/assets/c3b453b9-20a7-41eb-85c2-bc8982c8f359" />

---

## 📌 Tech Stack

* **Python**
* **Streamlit**
* **Custom analytics modules (my_pages)**

---

## 📜 Version

**Simplitics – LIA Monitoring Dashboard**
Version: `1.0`
Year: `2025`

---

## ⚠ License

This project is proprietary and confidential.
© 2025 Simplitics. All rights reserved.
