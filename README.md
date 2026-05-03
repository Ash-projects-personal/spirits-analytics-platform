# spirits-analytics-platform

A full-stack data warehouse and analytics pipeline I built to analyze sales depletion data for the alcohol/spirits industry. Pushing the core ETL logic and SQL schema here.

## What this does

It takes raw transaction data (simulated here for 27+ brands across 11 countries) and builds a clean SQLite star schema data warehouse. The really tricky part of alcohol analytics is dealing with NABCA control states vs open markets, so I built the logic to automatically apply statutory volume discounts (usually around 30%) based on the geography dimension.

Once the data is clean, the analysis script runs queries to find margin improvement opportunities. In the sample data run here, it identifies a 14% margin improvement opportunity by shifting volume into 3 underpenetrated on-premise channels (Bars, Restaurants, Hotels).

## The numbers

- **Scale**: Handles 100k+ transactions across 27 brands
- **Insight**: Surfaced a 14.2% margin opportunity in on-premise channels
- **Architecture**: Star schema with 3 dimension tables (Brand, Geography, Channel) and 1 fact table (Depletions)

## How to run

```bash
pip install -r requirements.txt
python etl_analysis.py
```

This will:
1. Create `warehouse.db` (the SQLite star schema)
2. Generate and load 100,000 transaction records
3. Run the SQL updates for COGS, control-state discounts, and net margin
4. Execute the analytics queries and generate a chart in `outputs/`

## Files

- `etl_analysis.py`: The main ETL and analytics script
- `warehouse.db`: The generated SQLite database (created after running)
- `outputs/channel_margin_analysis.png`: Visualization of the margin opportunity
- `outputs/executive_summary.md`: The final business insights
