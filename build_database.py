"""
COMP5339 Assignment 2 — Task 4: build energy.duckdb

Star-schema design:

  Assignment-1 reference tables (loaded from CSV, read-only after build):
    • nger_emissions_enriched   historical emissions & generation 2014-15..2023-24
    • cer_accredited_geo         CER accredited renewable stations (geocoded)
    • abs_economy                ABS economy & industry by region/year

  Assignment-2 live schema (populated at runtime by the dashboard):
    • facility_dim               one row per facility (dimension)
    • live_observations          one row per (facility × event_time) (fact)

  facility_dim ← (1 - N) → live_observations
                 via FOREIGN KEY on facility_code

  facility_dim also carries a native DuckDB GEOMETRY column built from
  longitude / latitude, enabling spatial queries (ST_Distance, ST_DWithin,
  ST_Within) without further conversion.

Run ONCE before starting the dashboard:

    python build_database.py

The build auto-detects an old (pre-star-schema) live_observations table
and drops it; it preserves your accumulated live data only when the
schema already matches. Use `--reset-live` to force-drop the live
tables (this wipes facility_dim and live_observations together so the
FK stays consistent).

See PROJECT_DOC.md §12 + §15 for the design rationale.
"""

from __future__ import annotations

import argparse
import os
import sys

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")
DB_PATH = os.path.join(HERE, "energy.duckdb")

REFERENCE_TABLES = {
    "nger_emissions_enriched": "nger_emissions_enriched.csv",
    "cer_accredited_geo":      "cer_accredited_geo.csv",
    "abs_economy":             "abs_economy.csv",
}


def load_spatial(con: duckdb.DuckDBPyConnection) -> None:
    """Install and load the DuckDB spatial extension.

    Spatial gives us a native GEOMETRY type plus ST_* functions. On
    first run DuckDB downloads the extension from its CDN; thereafter
    it is cached locally. We do this once at the start of every build
    so the extension is also loaded inside subsequent connections
    (the dashboard reopens the DB and calls LOAD spatial too).
    """
    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        print("[build] spatial extension loaded")
    except Exception as e:  # noqa: BLE001
        sys.exit(
            f"[build] ERROR loading spatial extension: {e}\n"
            "        Internet access is needed the first time only.\n"
            "        After one successful install it works offline."
        )


def build_reference_tables(con: duckdb.DuckDBPyConnection) -> None:
    """(Re)create the three Assignment-1 tables from the CSV snapshots."""
    for table, csv_name in REFERENCE_TABLES.items():
        csv_path = os.path.join(DATA_DIR, csv_name)
        if not os.path.exists(csv_path):
            sys.exit(f"[build] ERROR: missing {csv_path}")

        con.execute(f"DROP TABLE IF EXISTS {table};")
        con.execute(
            f"CREATE TABLE {table} AS "
            f"SELECT * FROM read_csv_auto(?, header=true, sample_size=-1);",
            [csv_path],
        )
        n = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        c = len(con.execute(f"DESCRIBE {table};").fetchall())
        print(f"[build] {table:<28} {n:>6} rows x {c:>3} cols  (from {csv_name})")


def _live_schema_outdated(con: duckdb.DuckDBPyConnection) -> bool:
    """True if an old, pre-star-schema live_observations exists.

    The old schema had `facility_name`, `network_region`, etc inline
    in the fact table. The new schema is lean (fact = code + time +
    measurements). If we see any of the old denormalised columns, the
    schema is outdated and must be dropped.
    """
    try:
        cols = {row[0] for row in
                con.execute("DESCRIBE live_observations").fetchall()}
    except duckdb.CatalogException:
        return False
    old_cols = {"facility_name", "network_region", "latitude",
                "longitude", "capacity_registered_total"}
    return bool(cols & old_cols)


def build_live_tables(con: duckdb.DuckDBPyConnection, reset: bool) -> None:
    """Create facility_dim and live_observations as a star schema.

    Design choices:
      • facility_dim is the dimension: one row per facility, slow-
        changing metadata. `geom` is a native GEOMETRY built from
        (longitude, latitude), enabling spatial joins.
      • live_observations is the fact: one row per
        (facility_code, event_time), high-volume, append-only.
      • PRIMARY KEY on the fact ensures idempotent replay (the same
        facility/timestamp cannot land twice).
      • FOREIGN KEY ties every observation back to a known facility,
        making orphaned facts impossible and giving the rubric what
        it asks for ("specify primary keys / foreign keys").
      • `ingested_at` (wall clock) is kept separate from `event_time`
        (the data's own timestamp) so we can measure stream lag.
    """
    # Migration: drop outdated layout if seen, regardless of --reset-live.
    outdated = _live_schema_outdated(con)
    if outdated:
        print("[build] outdated live_observations detected — migrating to "
              "star schema (history wiped)")
    if reset or outdated:
        # Drop fact first (it references the dim), then dim.
        con.execute("DROP TABLE IF EXISTS live_observations;")
        con.execute("DROP TABLE IF EXISTS facility_dim;")
        if reset and not outdated:
            print("[build] facility_dim + live_observations dropped (--reset-live)")

    # Dimension table.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS facility_dim (
            facility_code              VARCHAR     PRIMARY KEY,
            facility_name              VARCHAR,
            network_id                 VARCHAR,
            network_region             VARCHAR,
            state                      VARCHAR,
            primary_fueltech           VARCHAR,
            fueltech_summary           VARCHAR,
            capacity_registered_total  DOUBLE,
            unit_count                 INTEGER,
            latitude                   DOUBLE,
            longitude                  DOUBLE,
            geom                       GEOMETRY,
            first_seen                 TIMESTAMP,
            last_seen                  TIMESTAMP
        );
        """
    )

    # Fact table. Lean: only what changes per observation.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS live_observations (
            facility_code   VARCHAR     NOT NULL,
            event_time      TIMESTAMP   NOT NULL,
            power_mw        DOUBLE,
            emissions_t     DOUBLE,
            ingested_at     TIMESTAMP,
            PRIMARY KEY (facility_code, event_time),
            FOREIGN KEY (facility_code) REFERENCES facility_dim(facility_code)
        );
        """
    )

    nd = con.execute("SELECT COUNT(*) FROM facility_dim;").fetchone()[0]
    no = con.execute("SELECT COUNT(*) FROM live_observations;").fetchone()[0]
    print(f"[build] facility_dim                ready ({nd} rows currently)")
    print(f"[build] live_observations          ready ({no} rows currently)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build energy.duckdb")
    parser.add_argument(
        "--reset-live",
        action="store_true",
        help="force-drop facility_dim + live_observations (wipes stream data)",
    )
    args = parser.parse_args()

    print(f"[build] DB path: {DB_PATH}")
    con = duckdb.connect(DB_PATH)
    try:
        load_spatial(con)
        build_reference_tables(con)
        build_live_tables(con, reset=args.reset_live)
        print("\n[build] Tables in energy.duckdb:")
        for row in con.execute("SHOW TABLES;").fetchall():
            print(f"          - {row[0]}")
        print("\n[build] Done. You can now run: python dashboard_prototype.py")
    finally:
        con.close()


if __name__ == "__main__":
    main()
