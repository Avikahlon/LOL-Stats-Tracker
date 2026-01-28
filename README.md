# 🧠 League of Legends Stats Tracker

A data-driven project that tracks and visualizes League of Legends player statistics by scraping data from external sources, storing it in a structured database, and delivering insights through **Power BI dashboards**.

---

## 🔍 Project Overview

Due to the lack of direct access to competitive League of Legends datasets, this project utilizes a custom-built scraper to extract performance metrics from trusted external sites. The data is processed through an ETL (Extract, Transform, Load) pipeline and stored in a relational database, providing a clean foundation for **advanced data visualization and trend analysis in Power BI**.



---

## ⚙️ Features

- **🕷️ Automated Web Scraper:** Collects player stats including KDA, gold per minute, and champion-specific win rates.
- **💾 Structured Storage:** Relational database architecture optimized for historical data tracking.
- **📊 Power BI Dashboards:** Interactive visualizations allowing for deep dives into player performance and tournament trends.
- **📉 Comparative Analytics:** Side-by-side player and team metrics to identify strengths and weaknesses.
- **🔄 Scheduled ETL:** Regular data updates managed via Airflow to ensure dashboards reflect the latest matches.

---

## 🧱 Tech Stack

### Data Engineering & Backend
- **Python:** (BeautifulSoup, Requests) for data extraction and cleaning.
- **Airflow:** For workflow orchestration and pipeline scheduling.
- **PostgreSQL:** As the primary data warehouse.

### Data Visualization
- **Power BI:** For building interactive reports, DAX modeling, and visual storytelling.

### Dev Tools
- **Git + GitHub:** Version control.
- **Docker:** Containerization for the scraping environment.
- **AWS:** Cloud hosting for the database and automation scripts.
