"""Download one full year of Montgomery County alcohol sales.

Dataset: Warehouse and Retail Sales, Montgomery County, Maryland
open data (Socrata dataset v76h-r7br). The county runs alcohol
distribution itself, a control jurisdiction, so these are the
county's own recorded sales: one row per item per month, with
retail store sales, warehouse (wholesale) sales and transfers,
in units. Source:
https://data.montgomerycountymd.gov/resource/v76h-r7br.json

Years present in the dataset when fetched (Oct 2026): 2017, 2018,
2019, 2020 and a partial 2026. This project uses 2019, the largest
complete year (138,638 rows), and says so wherever it reports.
Montgomery County publishes this data as open data for public use.
"""
import hashlib
import json
import pathlib

import requests

ROOT = pathlib.Path(__file__).resolve().parent.parent
BASE = "https://data.montgomerycountymd.gov/resource/v76h-r7br.json"
YEAR = "2019"
PAGE = 50000


def main():
    target = ROOT / "data" / f"montgomery_liquor_sales_{YEAR}.json"
    if target.exists():
        print("exists", target)
        return
    rows = []
    offset = 0
    while True:
        r = requests.get(BASE, params={
            "$limit": PAGE, "$offset": offset,
            "$where": f"calendar_year='{YEAR}'",
            "$order": "item_code, cal_month_num",
        }, timeout=120, headers={"User-Agent": "spirits-analytics-platform rebuild"})
        if r.status_code != 200:
            raise SystemExit(f"API error {r.status_code}: {r.text[:200]}")
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        print("page offset", offset, "batch", len(batch), "kept so far", len(rows))
        if len(batch) < PAGE:
            break
        offset += PAGE
    target.write_text(json.dumps(rows))
    print("saved", len(rows), "rows for", YEAR,
          "sha256", hashlib.sha256(target.read_bytes()).hexdigest())


if __name__ == "__main__":
    main()
