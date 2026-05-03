# spirits-analytics-platform

A full-stack data warehouse and analytics pipeline I built to analyze sales depletion data for the alcohol and spirits industry. Pushing the core ETL logic and SQL schema here.

It takes raw transaction data (simulated here for 27+ brands across 11 countries) and builds a clean SQLite star schema data warehouse. The really tricky part of alcohol analytics is dealing with NABCA control states vs open markets, so I built the logic to automatically apply statutory volume discounts (usually around 30%) based on the geography dimension.

Once the data is clean, the analysis script runs queries to find margin improvement opportunities. In the sample data run here, it identifies a 14% margin improvement opportunity by shifting volume into 3 underpenetrated on-premise channels (Bars, Restaurants, Hotels).

```bash
pip install -r requirements.txt
python etl_analysis.py
```

This will create warehouse.db (the SQLite star schema), generate and load 100,000 transaction records, run the SQL updates for COGS, control-state discounts, and net margin, execute the analytics queries, and generate a chart in outputs/.
