"""
Tests for the SQLite star-schema warehouse in etl_analysis.py.

We rebuild the warehouse in an isolated temp directory, then assert:
  * ETL row counts: fact and dimension tables hit their documented sizes
  * Star-schema FK integrity: every brand_id / geo_id / channel_id in the
    fact table is resolvable against the matching dim table
  * Derived financial fields stay arithmetically consistent
  * Control-state discount (NABCA 30%) is applied only to control-state geos
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pandas as pd
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from etl_analysis import generate_spirits_data  # noqa: E402


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    """Build the synthetic warehouse once for all assertions in this file."""
    tmp_dir = tmp_path_factory.mktemp("spirits-wh")
    db_path = str(tmp_dir / "warehouse.db")
    conn = generate_spirits_data(db_path=db_path)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# ETL row counts
# ---------------------------------------------------------------------------
def test_fact_depletions_row_count(warehouse):
    """The generator is documented to insert 100,000 transactions."""
    n = warehouse.execute("SELECT COUNT(*) FROM fact_depletions").fetchone()[0]
    assert n == 100_000


def test_dim_brand_row_count(warehouse):
    """The generator inserts 28 brands (IDs 1..28)."""
    n = warehouse.execute("SELECT COUNT(*) FROM dim_brand").fetchone()[0]
    assert n == 28


def test_dim_channel_row_count(warehouse):
    """Exactly 6 channels (3 off-premise + 3 on-premise)."""
    n = warehouse.execute("SELECT COUNT(*) FROM dim_channel").fetchone()[0]
    assert n == 6
    channel_types = {
        row[0]
        for row in warehouse.execute("SELECT DISTINCT channel_type FROM dim_channel").fetchall()
    }
    assert channel_types == {"Off-Premise", "On-Premise"}


def test_dim_geography_includes_usa_states_and_other_countries(warehouse):
    """USA gets per-state rows; the other 10 countries get a single 'All' row each."""
    rows = warehouse.execute("SELECT country, state, is_control_state FROM dim_geography").fetchall()
    by_country = {}
    for country, state, is_control in rows:
        by_country.setdefault(country, []).append((state, bool(is_control)))
    # USA gets 8 state rows (4 open + 4 mock control); 11 countries total.
    assert len(by_country) == 11
    assert len(by_country["USA"]) == 8
    control_states = [s for s, c in by_country["USA"] if c]
    assert set(control_states) == {"PA", "OH", "MI", "NC"}
    # All non-USA rows are open market.
    for country, entries in by_country.items():
        if country == "USA":
            continue
        assert entries == [("All", False)]


# ---------------------------------------------------------------------------
# Star-schema FK integrity
# ---------------------------------------------------------------------------
def test_every_fact_brand_id_resolves_in_dim_brand(warehouse):
    orphans = warehouse.execute(
        """
        SELECT COUNT(*) FROM fact_depletions f
        LEFT JOIN dim_brand b ON f.brand_id = b.brand_id
        WHERE b.brand_id IS NULL
        """
    ).fetchone()[0]
    assert orphans == 0


def test_every_fact_geo_id_resolves_in_dim_geography(warehouse):
    orphans = warehouse.execute(
        """
        SELECT COUNT(*) FROM fact_depletions f
        LEFT JOIN dim_geography g ON f.geo_id = g.geo_id
        WHERE g.geo_id IS NULL
        """
    ).fetchone()[0]
    assert orphans == 0


def test_every_fact_channel_id_resolves_in_dim_channel(warehouse):
    orphans = warehouse.execute(
        """
        SELECT COUNT(*) FROM fact_depletions f
        LEFT JOIN dim_channel c ON f.channel_id = c.channel_id
        WHERE c.channel_id IS NULL
        """
    ).fetchone()[0]
    assert orphans == 0


# ---------------------------------------------------------------------------
# Derived-field integrity
# ---------------------------------------------------------------------------
def test_net_revenue_equals_gross_minus_discount(warehouse):
    """net_revenue is derived from gross_revenue - discount_applied."""
    sample = pd.read_sql_query(
        "SELECT gross_revenue, discount_applied, net_revenue FROM fact_depletions LIMIT 2000",
        warehouse,
    )
    diff = (sample["gross_revenue"] - sample["discount_applied"]) - sample["net_revenue"]
    # Float-tolerance: 1e-6 of gross_revenue scale (~10-50k → 1e-4 absolute).
    assert diff.abs().max() < 1.0


def test_margin_equals_net_minus_cogs(warehouse):
    sample = pd.read_sql_query(
        "SELECT net_revenue, cogs, margin FROM fact_depletions LIMIT 2000",
        warehouse,
    )
    diff = (sample["net_revenue"] - sample["cogs"]) - sample["margin"]
    assert diff.abs().max() < 1.0


def test_cogs_is_forty_percent_of_gross(warehouse):
    """The ETL hard-codes COGS at 40% of gross."""
    sample = pd.read_sql_query(
        "SELECT gross_revenue, cogs FROM fact_depletions LIMIT 2000", warehouse
    )
    ratio = sample["cogs"] / sample["gross_revenue"]
    # Float tolerance band around 0.40.
    assert ratio.between(0.3999, 0.4001).all()


# ---------------------------------------------------------------------------
# Control-state discount logic
# ---------------------------------------------------------------------------
def test_control_states_get_thirty_percent_discount(warehouse):
    """All rows tied to a control-state geo have discount = 0.30 * gross."""
    rows = pd.read_sql_query(
        """
        SELECT f.gross_revenue, f.discount_applied
        FROM fact_depletions f
        JOIN dim_geography g ON f.geo_id = g.geo_id
        WHERE g.is_control_state = 1
        LIMIT 5000
        """,
        warehouse,
    )
    if rows.empty:
        pytest.skip("no control-state rows in sample")
    ratios = rows["discount_applied"] / rows["gross_revenue"]
    assert ratios.between(0.2999, 0.3001).all()


def test_open_market_geos_have_zero_discount(warehouse):
    """All rows tied to an open-market (is_control_state=0) geo have zero discount."""
    rows = pd.read_sql_query(
        """
        SELECT f.discount_applied
        FROM fact_depletions f
        JOIN dim_geography g ON f.geo_id = g.geo_id
        WHERE g.is_control_state = 0
        LIMIT 5000
        """,
        warehouse,
    )
    assert (rows["discount_applied"] == 0.0).all()


def test_sqlite_schema_declares_expected_foreign_keys(warehouse):
    """The fact table must declare FKs to the three dimensions (recruiter-readable schema)."""
    fks = warehouse.execute("PRAGMA foreign_key_list(fact_depletions)").fetchall()
    referenced = {row[2] for row in fks}
    assert referenced == {"dim_brand", "dim_geography", "dim_channel"}
