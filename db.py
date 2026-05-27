"""
COMP5339 Assignment 2 — Task 4: database persistence + integration.

This module is the single seam between the live MQTT stream and the
DuckDB database built by build_database.py. It does two jobs:

  1. persist_observation(msg)
     Write one MQTT message into live_observations (idempotent).

  2. get_integration(facility_name, state)
     Join the live facility against the Assignment-1 reference tables to
     answer the report question "how do you integrate the MQTT messages
     with your existing data from Assignment 1?". Returns historical
     emissions for that facility (from nger_emissions_enriched) plus the
     economic context of its state (from abs_economy).

Design notes (see PROJECT_DOC §12):

  • DuckDB allows only one read-write connection per process and its
    connections are not safe to share across threads without
    serialisation. The MQTT thread writes and the Dash callback thread
    reads, so every DB access goes through a single module-level
    connection guarded by one lock. Throughput here is ~10 msg/s, so a
    lock is more than adequate.

  • The MQTT-stream `network_region` is a NEM region code (NSW1, VIC1,
    QLD1, SA1, TAS1). The Assignment-1 tables key on a state code (NSW,
    VIC, QLD, SA, TAS). The mapping is "strip the trailing digit". This
    is THE integration key and is documented as such.

  • Facility-name matching between the stream and nger is fuzzy: the
    stream says "Bayswater" while nger says "BAYSWATER POWER STATION".
    We uppercase both and match where the nger name CONTAINS the stream
    name, narrowed by state. This is imperfect by nature and the limit
    is documented (report: "challenges encountered").
"""

from __future__ import annotations

import os
import re
import threading
from datetime import datetime
from typing import Any

import duckdb

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "energy.duckdb")

_DB_LOCK = threading.Lock()
_CON: duckdb.DuckDBPyConnection | None = None


# --------------------------------------------------------------------------
# Connection management
# --------------------------------------------------------------------------

def _connect() -> duckdb.DuckDBPyConnection:
    """Open (once) the shared connection. Raises if the DB is missing.

    Loads the spatial extension on every fresh connection so writes
    using ST_Point() and reads of GEOMETRY columns work. INSTALL was
    already done by build_database.py; LOAD is cheap and idempotent.
    """
    global _CON
    if _CON is None:
        if not os.path.exists(DB_PATH):
            raise FileNotFoundError(
                f"{DB_PATH} not found. Run `python build_database.py` first."
            )
        _CON = duckdb.connect(DB_PATH)
        try:
            _CON.execute("LOAD spatial;")
        except Exception as e:  # noqa: BLE001
            # Non-fatal: persistence still works, only spatial reads
            # would fail. Log and continue.
            print(f"[db] WARNING: spatial extension not loaded: {e}")
    return _CON


def db_available() -> bool:
    """True if energy.duckdb exists and can be opened."""
    try:
        with _DB_LOCK:
            _connect()
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[db] unavailable: {e}")
        return False


def close() -> None:
    global _CON
    with _DB_LOCK:
        if _CON is not None:
            _CON.close()
            _CON = None


# --------------------------------------------------------------------------
# Integration key helpers
# --------------------------------------------------------------------------

def region_to_state(network_region: str | None) -> str | None:
    """NEM region code -> Assignment-1 state code.

    'NSW1' -> 'NSW', 'SA1' -> 'SA', 'TAS1' -> 'TAS'. This is the single
    join key between the live stream and the Assignment-1 tables.
    """
    if not network_region:
        return None
    m = re.match(r"^([A-Z]+)\d*$", network_region.strip().upper())
    return m.group(1) if m else network_region.strip().upper()


# --------------------------------------------------------------------------
# 1. Persistence
# --------------------------------------------------------------------------

def persist_observation(msg: dict[str, Any]) -> None:
    """Persist one MQTT message into the star schema.

    Two-step write under a single lock:
      1. UPSERT into facility_dim so the FK on live_observations can
         be satisfied (and so first_seen / last_seen track the
         lifecycle of each facility).
      2. INSERT OR REPLACE into live_observations.

    Safe to call from the MQTT thread: all exceptions are swallowed
    and logged so a DB hiccup never kills the subscriber loop.
    """
    try:
        facility_code = msg.get("facility_code")
        if not facility_code:
            return
        event_time = msg.get("event_time")
        if not event_time:
            # Defensive: skip messages without a timestamp — the fact
            # PK is (facility_code, event_time), NULL would fail.
            return

        # Derive the primary fueltech (see PROJECT_DOC §5.1):
        # the first non-charging/discharging token in the pipe-
        # separated summary.
        ft_summary = msg.get("fueltech_summary") or ""
        primary_ft = "unknown"
        for p in [x for x in ft_summary.split("|") if x]:
            if not p.endswith("_charging") and not p.endswith("_discharging"):
                primary_ft = p
                break
        else:
            parts = [x for x in ft_summary.split("|") if x]
            if parts:
                primary_ft = parts[0]

        state = region_to_state(msg.get("network_region"))
        lat = msg.get("latitude")
        lon = msg.get("longitude")
        now = datetime.now()

        dim_row = (
            facility_code,
            msg.get("facility_name"),
            msg.get("network_id"),
            msg.get("network_region"),
            state,
            primary_ft,
            ft_summary or None,
            msg.get("capacity_registered_total"),
            msg.get("unit_count"),
            lat,
            lon,
            now,  # first_seen — only used on insert (see ON CONFLICT below)
            now,  # last_seen — overwritten every observation
        )
        fact_row = (
            facility_code,
            event_time,
            msg.get("power_mw"),
            msg.get("emissions_t"),
            now,
        )

        with _DB_LOCK:
            con = _connect()
            # --- dim: UPSERT --------------------------------------------
            # geom is built server-side from (lon, lat) using ST_Point,
            # which is the standard (X=lon, Y=lat) convention. ON
            # CONFLICT updates the slow-changing fields and last_seen
            # but preserves first_seen.
            con.execute(
                """
                INSERT INTO facility_dim (
                    facility_code, facility_name, network_id, network_region,
                    state, primary_fueltech, fueltech_summary,
                    capacity_registered_total, unit_count,
                    latitude, longitude, geom, first_seen, last_seen
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,
                          CASE WHEN ? IS NOT NULL AND ? IS NOT NULL
                               THEN ST_Point(?, ?) ELSE NULL END,
                          ?, ?)
                ON CONFLICT (facility_code) DO UPDATE SET
                    facility_name             = EXCLUDED.facility_name,
                    network_id                = EXCLUDED.network_id,
                    network_region            = EXCLUDED.network_region,
                    state                     = EXCLUDED.state,
                    primary_fueltech          = EXCLUDED.primary_fueltech,
                    fueltech_summary          = EXCLUDED.fueltech_summary,
                    capacity_registered_total = EXCLUDED.capacity_registered_total,
                    unit_count                = EXCLUDED.unit_count,
                    latitude                  = EXCLUDED.latitude,
                    longitude                 = EXCLUDED.longitude,
                    geom                      = EXCLUDED.geom,
                    last_seen                 = EXCLUDED.last_seen
                """,
                (
                    *dim_row[:11],   # facility_code .. longitude
                    lon, lat,        # geom guard: both must be non-null
                    lon, lat,        # ST_Point(lon, lat)
                    dim_row[11],     # first_seen
                    dim_row[12],     # last_seen
                ),
            )
            # --- fact: INSERT OR REPLACE -------------------------------
            # Idempotent on (facility_code, event_time). FK to dim is
            # satisfied because we just upserted the parent row.
            con.execute(
                """
                INSERT OR REPLACE INTO live_observations
                    (facility_code, event_time, power_mw, emissions_t, ingested_at)
                VALUES (?,?,?,?,?)
                """,
                fact_row,
            )
    except Exception as e:  # noqa: BLE001
        print(f"[db] persist failed for {msg.get('facility_code')}: {e}")


# --------------------------------------------------------------------------
# 2. Integration query (used by the click popup)
# --------------------------------------------------------------------------

def get_integration(facility_name: str | None, network_region: str | None) -> dict:
    """Join a live facility to the Assignment-1 reference tables.

    Returns a dict with:
      • nger:  most recent historical emissions row for this facility
               (fuzzy name match within state), or None
      • nger_years: how many financial years of NGER history exist
      • abs:   the state's latest economic snapshot, or None

    Never raises — returns empty fields on any error so the popup still
    renders the live data.
    """
    result: dict[str, Any] = {"nger": None, "nger_years": 0, "abs": None}
    if not facility_name:
        return result
    state = region_to_state(network_region)

    try:
        with _DB_LOCK:
            con = _connect()
            name_uc = facility_name.strip().upper()

            # --- NGER: fuzzy name match, narrowed by state -------------
            # Match where the full NGER name contains the (shorter)
            # stream name, e.g. 'BAYSWATER' ⊂ 'BAYSWATER POWER STATION'.
            nger = con.execute(
                """
                SELECT facility_name, primary_fuel, year_range,
                       total_emissions_tco2e, emission_intensity,
                       electricity_production_mwh
                FROM nger_emissions_enriched
                WHERE type IN ('F', 'FA')
                  AND (UPPER(facility_name) LIKE '%' || ? || '%'
                       OR ? LIKE '%' || UPPER(facility_name) || '%')
                  AND (? IS NULL OR UPPER(state) = ?)
                ORDER BY year_range DESC
                LIMIT 1
                """,
                [name_uc, name_uc, state, state],
            ).fetchone()

            if nger:
                result["nger"] = {
                    "facility_name": nger[0],
                    "primary_fuel": nger[1],
                    "year_range": nger[2],
                    "total_emissions_tco2e": nger[3],
                    "emission_intensity": nger[4],
                    "electricity_production_mwh": nger[5],
                }
                # how many years of history matched (depth of integration)
                yc = con.execute(
                    """
                    SELECT COUNT(DISTINCT year_range)
                    FROM nger_emissions_enriched
                    WHERE type IN ('F','FA')
                      AND (UPPER(facility_name) LIKE '%' || ? || '%'
                           OR ? LIKE '%' || UPPER(facility_name) || '%')
                      AND (? IS NULL OR UPPER(state) = ?)
                    """,
                    [name_uc, name_uc, state, state],
                ).fetchone()
                result["nger_years"] = yc[0] if yc else 0

            # --- ABS: state economic context (latest year) ------------
            if state:
                abs_row = con.execute(
                    """
                    SELECT state, year,
                           total_number_of_businesses,
                           total_persons_employed_aged_15_years_and_over
                    FROM abs_economy
                    WHERE geographic_level = 'state'
                      AND UPPER(state) = ?
                    ORDER BY year DESC
                    LIMIT 1
                    """,
                    [state],
                ).fetchone()
                if abs_row:
                    result["abs"] = {
                        "state": abs_row[0],
                        "year": abs_row[1],
                        "total_number_of_businesses": abs_row[2],
                        "total_persons_employed": abs_row[3],
                    }
    except Exception as e:  # noqa: BLE001
        print(f"[db] integration query failed for {facility_name}: {e}")

    return result


def stream_stats() -> dict:
    """Small summary of what has been persisted (for the header / report)."""
    try:
        with _DB_LOCK:
            con = _connect()
            row = con.execute(
                """
                SELECT COUNT(*)                      AS observations,
                       COUNT(DISTINCT facility_code)  AS facilities,
                       MIN(event_time)                AS first_event,
                       MAX(event_time)                AS last_event
                FROM live_observations
                """
            ).fetchone()
            return {
                "observations": row[0],
                "facilities": row[1],
                "first_event": row[2],
                "last_event": row[3],
            }
    except Exception as e:  # noqa: BLE001
        print(f"[db] stream_stats failed: {e}")
        return {"observations": 0, "facilities": 0,
                "first_event": None, "last_event": None}


# --------------------------------------------------------------------------
# 3. Facility history (drives the per-facility generation chart)
# --------------------------------------------------------------------------

def get_facility_history(facility_code: str | None,
                         limit: int = 120) -> list[tuple]:
    """Return the most recent observations for one facility, oldest first.

    Used by the popup to render a live-updating generation chart. Up to
    `limit` rows (default 120 = ~10 hours at 5-min spacing). Returns
    [(event_time, power_mw, emissions_t), ...]; never raises.

    Extension point: a future market-price/demand layer could be joined
    in here as additional columns without changing the call site.
    """
    if not facility_code:
        return []
    try:
        with _DB_LOCK:
            con = _connect()
            rows = con.execute(
                """
                SELECT event_time, power_mw, emissions_t
                FROM (
                    SELECT event_time, power_mw, emissions_t
                    FROM live_observations
                    WHERE facility_code = ?
                    ORDER BY event_time DESC
                    LIMIT ?
                ) t
                ORDER BY event_time ASC
                """,
                [facility_code, limit],
            ).fetchall()
            return rows
    except Exception as e:  # noqa: BLE001
        print(f"[db] history query failed for {facility_code}: {e}")
        return []
