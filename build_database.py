"""
COMP5339 Assignment 2 — Task 4: build energy.duckdb

Creates a single DuckDB database that holds:

  Assignment-1 reference tables (loaded from CSV, read-only after build):
    • nger_emissions_enriched   historical emissions & generation 2014-15..2023-24
    • cer_accredited_geo         CER accredited renewable stations (geocoded)
    • abs_economy                ABS economy & industry by region/year

  Assignment-2 live table (populated at runtime by the dashboard):
    • live_observations          one row per facility per event_time from MQTT

Run ONCE before starting the dashboard:

    python build_database.py

It is idempotent: re-running drops and recreates the three reference
tables and (re)creates live_observations only if it does not exist, so
your accumulated live data is preserved across rebuilds. Use
`--reset-live` to also wipe live_observations.

See PROJECT_DOC.md §12 (Task 4) for the design rationale.
"""

from __future__ import annotations

import argparse
import os
import sys

import duckdb

# --------------------------------------------------------------------------
# Paths. The CSVs live in ./data next to this script. If you re-export a
# fuller abs_economy from Assignment 1, just overwrite data/abs_economy.csv
# and re-run this script — no code change needed.
# --------------------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")
DB_PATH = os.path.join(HERE, "energy.duckdb")

REFERENCE_TABLES = {
    "nger_emissions_enriched": "nger_emissions_enriched.csv",
    "cer_accredited_geo":      "cer_accredited_geo.csv",
    "abs_economy":             "abs_economy.csv",
}


def build_reference_tables(con: duckdb.DuckDBPyConnection) -> None:
    """(Re)create the three Assignment-1 tables from the CSV snapshots."""
    for table, csv_name in REFERENCE_TABLES.items():
        csv_path = os.path.join(DATA_DIR, csv_name)
        if not os.path.exists(csv_path):
            sys.exit(f"[build] ERROR: missing {csv_path}")

        con.execute(f"DROP TABLE IF EXISTS {table};")
        # read_csv_auto handles the quoting / embedded newlines in the
        # facility_description-style fields and infers types.
        con.execute(
            f"CREATE TABLE {table} AS "
            f"SELECT * FROM read_csv_auto(?, header=true, sample_size=-1);",
            [csv_path],
        )
        n = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        c = len(con.execute(f"DESCRIBE {table};").fetchall())
        print(f"[build] {table:<28} {n:>6} rows x {c:>3} cols  (from {csv_name})")


def build_live_table(con: duckdb.DuckDBPyConnection, reset: bool) -> None:
    """Create the live_observations table for the MQTT stream.

    Schema rationale (see PROJECT_DOC §12):
      • Primary key (facility_code, event_time) makes re-sent messages
        idempotent — the same facility/timestamp can only exist once.
      • `state` is the NEM-region-stripped state code (SA1 -> SA), the
        single join key into the Assignment-1 reference tables.
      • `ingested_at` (wall clock) is kept separate from `event_time`
        (the data's own timestamp) so we can tell stream lag.
    """
    if reset:
        con.execute("DROP TABLE IF EXISTS live_observations;")
        print("[build] live_observations dropped (--reset-live)")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS live_observations (
            facility_code              VARCHAR,
            facility_name              VARCHAR,
            network_id                 VARCHAR,
            network_region             VARCHAR,
            state                      VARCHAR,
            event_time                 TIMESTAMP,
            power_mw                   DOUBLE,
            emissions_t                DOUBLE,
            primary_fueltech           VARCHAR,
            fueltech_summary           VARCHAR,
            capacity_registered_total  DOUBLE,
            unit_count                 INTEGER,
            latitude                   DOUBLE,
            longitude                  DOUBLE,
            ingested_at                TIMESTAMP,
            PRIMARY KEY (facility_code, event_time)
        );
        """
    )
    n = con.execute("SELECT COUNT(*) FROM live_observations;").fetchone()[0]
    print(f"[build] live_observations          ready ({n} rows currently)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build energy.duckdb")
    parser.add_argument(
        "--reset-live",
        action="store_true",
        help="also drop and recreate live_observations (wipes accumulated stream data)",
    )
    args = parser.parse_args()

    print(f"[build] DB path: {DB_PATH}")
    con = duckdb.connect(DB_PATH)
    try:
        build_reference_tables(con)
        build_live_table(con, reset=args.reset_live)
        print("\n[build] Tables in energy.duckdb:")
        for row in con.execute("SHOW TABLES;").fetchall():
            print(f"          - {row[0]}")
        print("\n[build] Done. You can now run: python dashboard_prototype.py")
    finally:
        con.close()


if __name__ == "__main__":
    main()
