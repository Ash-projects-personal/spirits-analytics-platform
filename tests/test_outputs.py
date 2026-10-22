import json
import pathlib
import sqlite3

ROOT = pathlib.Path(__file__).resolve().parent.parent


def test_metrics_match_warehouse():
    m = json.loads((ROOT / "outputs" / "metrics.json").read_text())
    con = sqlite3.connect(ROOT / "warehouse.db")
    n = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    total = con.execute("""SELECT SUM(f.retail_units + f.warehouse_units) FROM fact_sales f
        JOIN dim_item i ON i.item_code = f.item_code
        WHERE i.item_type NOT IN ('REF', 'DUNNAGE')""").fetchone()[0]
    con.close()
    assert n == m["dataset"]["rows"]
    assert abs(total - m["totals"]["total_sales_units"]) < 1.0
    assert m["dataset"]["year_used"] == 2019


def test_summary_has_no_invented_margin():
    text = (ROOT / "outputs" / "executive_summary.md").read_text().lower()
    assert "14.2" not in text and "margin improvement" not in text
