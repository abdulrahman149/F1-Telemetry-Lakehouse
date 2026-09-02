# F1 Telemetry Lakehouse 🏎️📊

An end-to-end data engineering and business intelligence platform designed to ingest, process, and visualize high-frequency Formula 1 telemetry and race strategy data. 

This project bridges the gap between backend data infrastructure and front-end analytics, processing noisy, high-frequency sensor data into a clean Star Schema for a custom Power BI "Mission Control" dashboard.

### The Pipeline Flow
1. **Extraction (FastF1 API):** Pulls raw telemetry (speed, throttle, brake, gear) and lap timing data for specific Grand Prix sessions.
2. **Transformation (Apache Beam):** 
   * Downsamples and interpolates erratic sensor pings into a standardized 5-meter track grid for perfect head-to-head alignment.
   * Calculates a mathematical fuel-burn penalty (0.038s per lap) to expose true tire wear.
3. **Data Warehouse (PostgreSQL):** Clean data is micro-batched into a highly optimized Star Schema (Fact and Dimension tables).
4. **Visualization (Power BI):** A dark-mode analytics application built on top of the database.

## 📊 Key Analytics Features

* **Dynamic Speed Delta Engine:** Uses disconnected tables and custom DAX measures to calculate the exact km/h difference between any two selected drivers at specific track distances, visualized via a dual-axis trace.
* **Fuel-Corrected Tire Degradation:** Adjusts raw lap times to account for the car getting lighter as fuel burns, revealing the true degradation slope of different Pirelli tire compounds (Soft, Medium, Hard).
* **Race Stint Timeline:** A custom 100% stacked bar chart acting as a timeline for pit strategy and tire life across the 50-lap race distance.

## 💻 Tech Stack
* **Data Engineering:** Python, Apache Beam (`DirectRunner`), Pandas, Numpy
* **Database:** PostgreSQL (Star Schema, `psycopg2` bulk loading)
* **Analytics:** Microsoft Power BI, DAX (Data Analysis Expressions)
* **Domain Tools:** FastF1
