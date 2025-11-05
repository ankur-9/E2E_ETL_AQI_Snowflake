# E2E_ETL_AQI_Snowflake

🌏 Real-Time India Air Quality Index (AQI) – End-to-End Snowflake Data Engineering Project

📘 Project Overview
This project showcases an end-to-end real-time data engineering pipeline built on Snowflake, leveraging Dynamic Tables for continuous data transformation and Streamlit for interactive visualization. The system processes India’s real-time Air Quality Index (AQI) data from the Government of India Open Data Platform, updates it hourly, and provides city and station-level AQI insights.


🧠 Objective
To design a fully automated, scalable, and near real-time data pipeline that:
Ingests and processes AQI data every hour.
Cleans and transforms nested JSON data efficiently using Snowflake Dynamic Tables.
Computes AQI metrics and aggregates them at city and day levels.
Provides live insights through a Streamlit dashboard.
Uses GitHub Actions for automated hourly ingestion scheduling.


🏗️ Architecture Overview
1️⃣ Data Source:
Real-Time AQI API — provides hourly pollutant readings from multiple monitoring stations across Indian cities.

2️⃣ Ingestion Layer:
Python script connects to the API and loads raw JSON data to Snowflake Internal Stage.
A Snowflake Task copies staged data into a raw table in dev_db.stage_schema.

3️⃣ Transformation Layer (Dynamic Tables):
Clean Schema:
Dynamic tables parse and flatten JSON data where:
1 State → Multiple Cities
1 City → Multiple Stations
1 Station → 7 Pollutants (each with min, max, avg values)
Cleaned data stored at hourly granularity.
Consumption Schema:
Dynamic tables calculate AQI based on pollutant readings.
Created Dimension and Fact Tables:
location_dim
date_dim
aqi_fact
Aggregate tables for analysis:
agg_city_fact_hour_level
agg_city_fact_day_level

4️⃣ Visualization Layer:
Streamlit App connects directly to Snowflake.
Dropdown selectors for State → City → Station → Date.
Visualizations:
📈 Hourly AQI line chart
📊 Pollutant-level stacked bar chart
🗺️ Station geolocation map

5️⃣ Automation Layer:
GitHub Actions Workflow triggers Python ingestion script hourly.
Ensures the pipeline runs automatically without manual intervention.


⚙️ Tech Stack
Category	Tools / Services
Data Warehouse	🧊 Snowflake
Data Transformation	Snowflake Dynamic Tables
Scheduling	GitHub Actions
Data Ingestion	Python + Snowflake Connector
Visualization	Streamlit
Data Source	Government of India Open Data Portal (API)


📊 Key Features
✅ End-to-end ELT pipeline with automated hourly updates.
✅ Real-time AQI computation at station, city, and state levels.
✅ Full data lineage from API → Stage → Clean → Consumption → Dashboard.
✅ No manual refresh — powered by Dynamic Tables + GitHub Actions.
✅ Clean schema design with dimension and fact modeling.
✅ Interactive dashboard to visualize and explore air quality trends.


📂 Repository Structure
📁 india-aqi-snowflake-project
├── 📄 README.md
├── 🧩 aqi_ingest.py           # Python script for API data ingestion
├── 🧠 snowflake_scripts/
│   ├── create_db_schemas.sql
│   ├── create_dynamic_tables.sql
│   ├── create_tasks.sql
│   └── create_dimensions_facts.sql
├── 🧰 .github/
│   └── workflows/
│       └── aqi_ingestion.yml  # GitHub Actions workflow
├── 📊 streamlit_app/
│   ├── app.py                 # Streamlit dashboard code
│   └── utils.py
└── 🖼️ dashboard_screenshot.png


📈 Dashboard Preview
🚀 Future Enhancements
Add historical AQI trend analysis for year-over-year comparison.
Integrate alerts/notifications for critical pollution levels.
Enable multi-region data visualization for different states.
Deploy Streamlit app on Streamlit Cloud or AWS EC2 for public access.


💡 Key Learnings
Hands-on experience with Snowflake Dynamic Tables for incremental transformations.
Building modular ETL pipelines using Python + Snowflake Tasks.
Automating workflows using GitHub Actions.
Designing dim-fact schemas for real-time data models.
End-to-end ownership: ingestion, transformation, automation, and visualization.
