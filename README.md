
# Weather Data Warehouse Pipeline  
**Real-time Weather App to Databricks ETL – Running Daily on Free Tier**

![Pipeline Overview](https://img.shields.io/badge/Status-Running%20Daily-brightgreen)  
![Databricks](https://img.shields.io/badge/Databricks-Community%20Edition-ff3621)  
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Enabled-00bfff)  
![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C)

A clean, production-ready, **daily incremental** data pipeline that pulls live weather readings from my own Weather App (Supabase) and builds a complete **medallion architecture** inside **Databricks Community Edition (free tier)**.

Running perfectly every single day since **September 2025** with zero failures.

---

### Features
- Full **Medallion Architecture** (Bronze to Silver to Gold)
- Daily **incremental** processing (only last 2 days by default)
- One-click **full/backfill** load for initial setup
- PySpark + Delta Lake (ACID transactions, schema evolution, time travel ready)
- Comprehensive data quality checks & scoring
- Automatic schema creation with fallback for free-tier limitations
- Works 100% on **Databricks Community Edition**

---

### Tables Created

| Layer   | Table Name                                      | Purpose                                   | Write Mode   | Partitioned |
|---------|--------------------------------------------------|-------------------------------------------|--------------|-------------|
| Bronze  | `weather_warehouse.bronze_weather_data`   | Raw JSON from Supabase                    | Append       | No          |
| Silver  | `weather_warehouse.silver_weather_clean`         | Cleaned, enriched, validated, partitioned | Append       | By date     |
| Gold    | `weather_warehouse.gold_daily_summaries`         | Daily stats per city & date               | Append       | No          |
| Gold    | `weather_warehouse.gold_city_analytics`          | Latest city-level analytics               | Overwrite    | No          |

---

### Project Structure
```markdown
weather_pipeline/
├── main.py              Main entry points (daily job + initial load)
├── config.py            All configuration & secrets
├── extract.py           Supabase REST API ingestion
├── transform.py         Cleaning, validation, enrichment
├── analytics.py         Gold-layer aggregations
├── load.py              Write functions for all layers
├── utils.py             Logging & helper functions
└── README.md            This file
```

---

### How to Run

#### 1. Daily Scheduled Job (what Databricks runs every day)
```python
from main import run_daily_pipeline
run_daily_pipeline()
```

#### 2. First-Time Full Load / Reset
```python
from main import run_initial_load
run_initial_load()    # Uses overwrite mode – perfect for first run
```

#### 3. Quick Health Check
```python
from main import quick_setup_test
quick_setup_test()
```

---

### Current Status (December 2025)
- Started: September 2025  
- Success rate: **100%** (zero failures)  
- Fully automated with Databricks Job email notifications  
- Built and maintained entirely on **free tier**

---

### Limitations of Free Tier & My Next Learning Goals  
*(Inspired by an amazing comment on my first post)*

A senior data engineer once shared how they accidentally spun up a 4X-Large cluster just for testing and got a huge bill the next morning. That story made me realize: **free tier is perfect for learning, but production needs discipline**.

So here’s my upcoming study roadmap (already started!):
- Cluster types: Job clusters vs All-purpose vs Serverless
- Autoscaling & cluster policies
- Cost monitoring, budgets & alerts
- Delta Lake `OPTIMIZE` + `ZORDER` for storage & query cost savings
- Unity Catalog & data lineage (when available on paid)
- Databricks SQL + Genie (natural language dashboards)
- MLflow + weather forecasting model

I’m super excited to graduate to paid tier the **smart** way — with cost controls from day one.

---

### Acknowledgments
Huge thanks to the Databricks Community member whose “big bill horror story” became my biggest motivation to learn production-grade practices properly. One comment literally shaped my entire learning journey ahead!

---

**Never stop learning, especially in Data Engineering.**

Feel free to fork, star, or reach out if you're building something similar. Always happy to help fellow Bangladeshi data enthusiasts!

#DataEngineering #Databricks #PySpark #DeltaLake #MedallionArchitecture #ETL #FreeTierToProduction #DataScienceJourney #BangladeshDataCommunity

---
Made with passion by **Srayoshi Bashed Mirza**  
December 2025
