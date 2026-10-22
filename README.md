# Spirits analytics platform

I rebuilt this project from scratch. The old version simulated
100,000 transactions for invented brands, built a real SQLite star
schema on top of them, and then wrote a margin improvement figure
into its executive summary as a hardcoded string, whatever the data
said. The chart even drew a target line from the same constant.
This version keeps the part that was worth keeping, the star schema
ETL, and points it at real sales recorded by a real control
jurisdiction. Every number below is a query result.

## Dataset

Warehouse and Retail Sales, published as open data by Montgomery
County, Maryland. The county runs alcohol distribution itself, so
these are its own sales records: one row per item per month, with
units sold through the county's retail stores (rtl_sales), units
sold from the warehouse to licensees (whs_sales), and transfers
between stores (rtl_transfers, which are not sales).

Years in the dataset when I fetched it in October 2026: 2017,
2018, 2019, 2020 and a partial 2026. I use 2019, the largest
complete year: 138,638 rows and 23,698 items.
Source: https://data.montgomerycountymd.gov/resource/v76h-r7br.json
`src/download_data.py` is the fetch code, and the file hash is in
`data/data_card.json`.

Scope change, stated plainly: the dataset records units, not
dollars. There are no prices, costs or revenue in it. So this
version has no margin analysis at all, rather than a margin number
nobody measured. The old brand and country dimensions are gone
too. The real dimensions are item, supplier, type and time, and
the real channels are retail and warehouse.

## Warehouse

`src/run_analysis.py` builds warehouse.db, a SQLite star schema:

- dim_item: item_code, description, item_type, supplier
- dim_time: year, month
- fact_sales: retail, warehouse and transfer units per item per
  month

Two row types in the data are not products: REF (returns) and
DUNNAGE (deposit containers). They are loaded into the warehouse
but excluded from every sales total and reported separately in
`outputs/metrics.json` as adjustments.

## Findings for 2019, all computed

- Total recorded sales: 4,573,022 units. Retail stores sold
  959,931 units, the warehouse channel sold 3,613,091 units, and
  transfers were 957,382 units.
- Beer dominates volume: 3,223,647 units, 70.5% of the total, and
  only 8.1% of beer units went through the county's own stores.
  Most beer moves from the warehouse to licensees.
- Liquor is the mirror image: 406,538 units, 8.9% of the total,
  but 87.8% of liquor units were sold through the county's own
  retail stores. Wine sits between the two at 37.9% retail.
- Biggest month: May 2019, 484,317 units across both channels.
- Top item: CORONA EXTRA LOOSE NR - 12OZ, 154,379 units. Top
  supplier: CROWN IMPORTS, 823,112 units.

Tables are in `outputs/sales_by_type.csv`, `outputs/monthly_sales.csv`,
`outputs/top_items.csv` and `outputs/top_suppliers.csv`. The
executive summary in `outputs/executive_summary.md` is generated
by the code from the same query results. It contains no typed in
figures, and a test checks it stays that way.

A note on units: the county records some items in cases and some
in bottles, and the published file does not separate the two, so
unit totals mix the two exactly as published. Comparisons across
categories are still meaningful at this scale, but a single unit
here is a recorded sales unit, not a standardised bottle.

## Run it

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python src/download_data.py
python src/run_analysis.py
python -m pytest tests/
```

## Files

- `src/download_data.py` pages the county API for the 2019 rows.
- `src/run_analysis.py` builds the warehouse, runs every query,
  writes `outputs/` including the generated summary.
- `EXPLAIN.md` covers the schema choices and the REF and DUNNAGE
  handling.
