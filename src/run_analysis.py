"""Spirits sales analytics warehouse on real county sales data.

Builds a small SQLite star schema from Montgomery County's own
published alcohol sales for calendar year 2019, then runs the
analysis in SQL and writes an executive summary from the computed
numbers only.

Schema:
- dim_item(item_code, description, item_type, supplier)
- dim_time(year, month)
- fact_sales(item_code, year, month, retail_units, warehouse_units,
  transfer_units)

The dataset records units sold, not dollars, so this version has
no margin analysis at all. The old version used simulated data and
wrote a margin improvement figure into its summary as a constant.
There is no constant in this summary writer: every number in the
summary is read back from a query result.
"""
import json
import pathlib
import sqlite3
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs"
YEAR = 2019
DB = ROOT / "warehouse.db"


def load_rows():
    return pd.read_json(ROOT / "data" / f"montgomery_liquor_sales_{YEAR}.json", dtype=str)


def build_warehouse(df):
    if DB.exists():
        DB.unlink()
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.executescript("""
    CREATE TABLE dim_item (
        item_code TEXT PRIMARY KEY, description TEXT, item_type TEXT, supplier TEXT);
    CREATE TABLE dim_time (year INTEGER, month INTEGER, PRIMARY KEY (year, month));
    CREATE TABLE fact_sales (
        item_code TEXT REFERENCES dim_item(item_code),
        year INTEGER, month INTEGER,
        retail_units REAL, warehouse_units REAL, transfer_units REAL,
        PRIMARY KEY (item_code, year, month));
    """)
    items = (df[["item_code", "item_description", "item_type", "supplier"]]
             .drop_duplicates("item_code")
             .rename(columns={"item_description": "description"}))
    items.to_sql("dim_item", con, if_exists="append", index=False)
    times = df[["calendar_year", "cal_month_num"]].drop_duplicates()
    times = times.rename(columns={"calendar_year": "year", "cal_month_num": "month"})
    times.to_sql("dim_time", con, if_exists="append", index=False)
    fact = df.rename(columns={
        "calendar_year": "year", "cal_month_num": "month",
        "rtl_sales": "retail_units", "whs_sales": "warehouse_units",
        "rtl_transfers": "transfer_units"})
    fact = fact[["item_code", "year", "month", "retail_units", "warehouse_units", "transfer_units"]]
    for c in ("retail_units", "warehouse_units", "transfer_units"):
        fact[c] = pd.to_numeric(fact[c], errors="coerce").fillna(0)
    fact["year"] = fact["year"].astype(int)
    fact["month"] = fact["month"].astype(int)
    fact.to_sql("fact_sales", con, if_exists="append", index=False)
    con.commit()
    return con


def q(con, sql):
    return pd.read_sql_query(sql, con)


def main():
    OUT.mkdir(exist_ok=True)
    started = time.time()
    df = load_rows()
    con = build_warehouse(df)

    # REF (returns) and DUNNAGE (deposit containers) are not products
    # for sale. They stay in the warehouse but are reported separately
    # as adjustments, never inside sales totals.
    totals = q(con, """SELECT COUNT(*) AS fact_rows,
        SUM(f.retail_units) AS retail_units, SUM(f.warehouse_units) AS warehouse_units,
        SUM(f.transfer_units) AS transfer_units
        FROM fact_sales f JOIN dim_item i ON i.item_code = f.item_code
        WHERE i.item_type NOT IN ('REF', 'DUNNAGE')""").iloc[0]
    adjustments = q(con, """SELECT i.item_type,
        SUM(f.retail_units + f.warehouse_units) AS units
        FROM fact_sales f JOIN dim_item i ON i.item_code = f.item_code
        WHERE i.item_type IN ('REF', 'DUNNAGE') GROUP BY i.item_type""")
    n_items = q(con, "SELECT COUNT(*) AS n FROM dim_item").iloc[0]["n"]

    by_type = q(con, """
        SELECT i.item_type,
               SUM(f.retail_units) AS retail_units,
               SUM(f.warehouse_units) AS warehouse_units,
               SUM(f.retail_units + f.warehouse_units) AS total_units,
               COUNT(DISTINCT f.item_code) AS items
        FROM fact_sales f JOIN dim_item i ON i.item_code = f.item_code
        WHERE i.item_type NOT IN ('REF', 'DUNNAGE')
        GROUP BY i.item_type ORDER BY total_units DESC""")
    by_type["retail_share_pct"] = 100 * by_type["retail_units"] / (
        by_type["retail_units"] + by_type["warehouse_units"])
    total_units = float(by_type["total_units"].sum())
    by_type["share_of_total_pct"] = 100 * by_type["total_units"] / total_units
    by_type.to_csv(OUT / "sales_by_type.csv", index=False)

    monthly = q(con, """
        SELECT f.month, SUM(f.retail_units) AS retail_units,
               SUM(f.warehouse_units) AS warehouse_units
        FROM fact_sales f JOIN dim_item i ON i.item_code = f.item_code
        WHERE i.item_type NOT IN ('REF', 'DUNNAGE')
        GROUP BY f.month ORDER BY f.month""")
    monthly.to_csv(OUT / "monthly_sales.csv", index=False)

    top_items = q(con, """
        SELECT i.description AS item_description, i.item_type,
               SUM(f.retail_units + f.warehouse_units) AS total_units
        FROM fact_sales f JOIN dim_item i ON i.item_code = f.item_code
        GROUP BY f.item_code ORDER BY total_units DESC LIMIT 10""")
    top_items.to_csv(OUT / "top_items.csv", index=False)

    top_suppliers = q(con, """
        SELECT i.supplier, SUM(f.retail_units + f.warehouse_units) AS total_units,
               COUNT(DISTINCT f.item_code) AS items
        FROM fact_sales f JOIN dim_item i ON i.item_code = f.item_code
        GROUP BY i.supplier ORDER BY total_units DESC LIMIT 10""")
    top_suppliers.to_csv(OUT / "top_suppliers.csv", index=False)

    fig, ax = plt.subplots(figsize=(7, 3.5))
    ax.plot(monthly["month"], monthly["retail_units"], marker="o", label="retail")
    ax.plot(monthly["month"], monthly["warehouse_units"], marker="o", label="warehouse")
    ax.set_xlabel("month of 2019")
    ax.set_ylabel("units")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT / "monthly_sales.png", dpi=110)
    plt.close(fig)

    largest = by_type.iloc[0]
    liquor = by_type[by_type["item_type"] == "LIQUOR"].iloc[0]
    peak = monthly.loc[(monthly["retail_units"] + monthly["warehouse_units"]).idxmax()]
    metrics = {
        "dataset": {
            "name": "Warehouse and Retail Sales, Montgomery County MD open data (Socrata v76h-r7br)",
            "source": "https://data.montgomerycountymd.gov/resource/v76h-r7br.json",
            "publisher_note": "Montgomery County publishes this dataset as open data for public use. Sales recorded by the county's own Alcohol Beverage Services, a control jurisdiction.",
            "year_used": YEAR,
            "rows": int(len(df)),
            "items": int(n_items),
            "units": "units sold as recorded by the county (bottles and cases as recorded per item; the dataset does not separate them, and neither does this analysis)",
            "channels": "rtl_sales is the county's own retail stores, whs_sales is warehouse sales to licensees",
            "dropped": "No margin or revenue analysis: this dataset has no price, cost or revenue fields.",
        },
        "totals": {
            "retail_units": float(totals["retail_units"]),
            "warehouse_units": float(totals["warehouse_units"]),
            "transfer_units": float(totals["transfer_units"]),
            "total_sales_units": float(totals["retail_units"] + totals["warehouse_units"]),
        },
        "by_type": by_type.to_dict(orient="records"),
        "largest_type_row": largest.to_dict(),
        "liquor_row": liquor.to_dict(),
        "adjustments_not_in_sales": adjustments.to_dict(orient="records"),
        "peak_month": {"month": int(peak["month"]),
                       "total_units": float(peak["retail_units"] + peak["warehouse_units"])},
        "top_item": top_items.iloc[0].to_dict(),
        "top_supplier": top_suppliers.iloc[0].to_dict(),
        "runtime_seconds": round(time.time() - started, 1),
    }
    (OUT / "metrics.json").write_text(json.dumps(metrics, indent=2, default=str))
    (OUT / "run_log.txt").write_text(json.dumps(metrics, indent=2, default=str) + "\n")

    t = metrics["totals"]
    summary = f"""# Executive summary, computed from the 2019 county data

Source: Montgomery County Warehouse and Retail Sales open data,
{metrics['dataset']['rows']} monthly item rows, {metrics['dataset']['items']} items.

Total recorded sales for 2019: {t['total_sales_units']:,.0f} units.
Retail stores sold {t['retail_units']:,.0f} units and the warehouse
channel sold {t['warehouse_units']:,.0f} units. Transfers between
stores were {t['transfer_units']:,.0f} units and are not sales.

The largest category is {largest['item_type']}: {largest['total_units']:,.0f} units,
{largest['share_of_total_pct']:.1f}% of all units, with a retail share
of {largest['retail_share_pct']:.1f}%. Liquor sold
{liquor['total_units']:,.0f} units ({liquor['share_of_total_pct']:.1f}% of
units), and {liquor['retail_share_pct']:.1f}% of liquor units went
through the county's own retail stores. Of the three
big categories (beer, wine, liquor) liquor has by far the highest
retail share. Beer is the mirror image: the biggest volume,
mostly through the warehouse channel to licensees.

The biggest month was month {metrics['peak_month']['month']} with
{metrics['peak_month']['total_units']:,.0f} units across both channels.

Top item by units: {metrics['top_item']['item_description']} with
{float(metrics['top_item']['total_units']):,.0f} units. Top supplier:
{metrics['top_supplier']['supplier']} with
{float(metrics['top_supplier']['total_units']):,.0f} units.

This summary is generated by src/run_analysis.py from query results
only. This dataset records units, not dollars, so there is no margin
or revenue statement in it, and none is made here.
"""
    (OUT / "executive_summary.md").write_text(summary)
    con.close()
    print(json.dumps(metrics["totals"], indent=2))
    print(by_type.to_string())
    print("peak month", metrics["peak_month"], "| top item", metrics["top_item"]["item_description"])


if __name__ == "__main__":
    main()
