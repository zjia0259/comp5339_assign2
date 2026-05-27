# COMP5339 Assignment 2 — Task 5: Real-time Dashboard

> This document records the design decisions, architecture, and implementation
> choices for the **subscriber + dashboard** half of the project (Task 5).
> Tasks 1–3 (data retrieval / consolidation / MQTT publishing) are handled by
> a teammate; this document focuses on the consumer side.

---

## 1. Scope and Requirements

From the assignment brief (Task 5):

- Subscribe to MQTT messages published by Task 3.
- Build a **map-based dashboard** in Python, similar to
  `https://explore.openelectricity.org.au/facilities/nem/`.
- For each received message, dynamically add/update a marker at the facility's
  location.
- Each marker displays the station's name and current **power output** *or*
  **emissions** (switchable).
- Clicking a marker shows a popup with name, type, latest power, latest
  emissions.
- **Optional:** filter by network region and fuel technology; show market
  price and demand if Task 1's optional sub-task was done.

---

## 2. System Overview

```
┌──────────────┐   MQTT publish    ┌──────────────────┐
│  Teammate's  │ ────────────────► │  HiveMQ public   │
│  publisher   │  topic: comp5339/ │  broker          │
│  (Tasks 1-3) │  electricity/...  │  (broker.hivemq  │
└──────────────┘                   │   .com:1883)     │
                                   └────────┬─────────┘
                                            │ MQTT subscribe
                                            ▼
                          ┌───────────────────────────────────┐
                          │  Subscriber thread (paho-mqtt)    │
                          │  on_message → parse JSON          │
                          │  → write to shared state dict     │
                          │     keyed by facility_code        │
                          └─────────────────┬─────────────────┘
                                            │ (thread-safe read)
                                            ▼
                          ┌───────────────────────────────────┐
                          │  Dash app (main thread)           │
                          │  • dcc.Interval polls state       │
                          │  • plotly scatter_map renders     │
                          │    markers                        │
                          │  • Click → popup with details     │
                          │  • Controls: power/emissions      │
                          │    toggle, region & fueltech      │
                          │    filters                        │
                          └─────────────────┬─────────────────┘
                                            │ HTTP
                                            ▼
                                      Browser (user)
```

### Key design choice: decoupled threads

- The MQTT subscriber runs in a **background daemon thread** (`loop_start()`),
  not `loop_forever()` which would block the main thread.
- It writes into a single shared dict `STATE: dict[facility_code, latest_msg]`,
  protected by a `threading.Lock`.
- The Dash app reads a snapshot of this dict every refresh tick (default 2 s).
  This means:
  - The dashboard never blocks on MQTT.
  - The MQTT side never blocks on rendering.
  - Multiple identical messages for the same facility are deduplicated
    naturally — the dict only ever holds the **latest** record per facility.

---

## 3. Technology Stack

| Component       | Choice              | Rationale                                                                                                 |
|-----------------|---------------------|-----------------------------------------------------------------------------------------------------------|
| Web framework   | **Dash (Plotly)**   | Native map support via `scatter_map`; `dcc.Interval` is the canonical pattern for live updates; one file. |
| Map rendering   | **Plotly Maplibre** | Free, no API key required (uses `open-street-map` style). Built into Plotly ≥5.24.                        |
| MQTT client     | **paho-mqtt 2.x**   | Python de-facto standard. Used with v2 callback API to avoid deprecation warnings.                        |
| Concurrency     | **threading**       | Simple, sufficient for this throughput (~1 facility/0.1s).                                                |
| HTML stripping  | **stdlib `html.parser`** | Avoids pulling in BeautifulSoup just for stripping `<p>` tags from descriptions.                     |

**Not chosen and why:**
- *Streamlit*: live updates require `st.rerun()` hacks and don't play nicely with
  background threads; the user model is "re-run script on every interaction"
  which fights against the streaming use case.
- *Folium / Leaflet*: would need a separate frontend layer; Dash + Plotly is
  more integrated.

---

## 4. MQTT Message Schema

The teammate's publisher sends one JSON message per facility per round, on
topic `comp5339/electricity/facility/<...>`. **Each message already contains
the facility metadata** (lat/lon, fueltech, capacity, description, units),
so the dashboard does **not** need to read Assignment 1's facility CSV at
runtime. This simplifies deployment substantially.

### Fields used by the dashboard

| Field                       | Type       | Use                                                       |
|-----------------------------|------------|-----------------------------------------------------------|
| `facility_code`             | str        | Primary key in the state dict                             |
| `facility_name`             | str        | Marker label, popup title                                 |
| `network_id`                | str        | Always `"NEM"` for this project                           |
| `network_region`            | str        | Filter dimension (`NSW1`, `VIC1`, `QLD1`, `SA1`, `TAS1`)  |
| `fueltech_summary`          | str        | Pipe-separated, e.g. `"battery|battery_charging|solar_utility"` |
| `power_mw`                  | float      | Live metric, can be negative (charging / aux load)        |
| `emissions_t`               | float      | Live metric                                               |
| `capacity_registered_total` | float      | Marker size                                               |
| `latitude`, `longitude`     | float      | Marker position                                           |
| `unit_count`                | int        | Shown in popup                                            |
| `event_time`                | ISO 8601   | Shown in popup, used for "last update" indicator          |
| `facility_description`      | str (HTML) | Excerpt shown in popup after HTML stripping               |
| `dispatch_type_summary`     | str        | Reserved — not yet rendered                               |
| `unit_status_summary`       | str        | Reserved — not yet rendered                               |
| `unit_details` (list)       | list[obj]  | Reserved — could be shown as a sub-table in the popup     |

### Topic structure observed

`comp5339/electricity/facility/<2-char>/<3-char>` — appears to be a random
suffix per publisher run (e.g. `.../jza/tky`). The subscriber uses the
wildcard `comp5339/electricity/facility/#` so any topic under that prefix
is captured.

---

## 5. Field-handling Decisions

These are non-obvious choices that shape the UX. Documented here so they
can be defended to the tutor / written into the report.

### 5.1 Picking a "primary" fueltech for colouring

`fueltech_summary` can be a pipe-separated bag like
`"battery|battery_charging|battery_discharging|solar_utility"`. For
colouring a marker we need one value.

**Rule:** take the first value that is **not** `battery_charging` /
`battery_discharging` (those are derivative views of `battery`). Falls back
to the first value if all are derivative.

Examples:
- `"gas_ocgt"` → `gas_ocgt`
- `"battery|battery_charging|battery_discharging|solar_utility"` → `battery`
- `"battery|battery_charging|wind"` → `battery`

### 5.2 Negative `power_mw`

Battery charging units and some biomass units report negative power
(e.g. `BWTR1` at `-25.32`, batteries during charge). Decisions:
- **Marker size:** uses `abs(power_mw)` (with a floor of capacity for
  stability — see 5.4).
- **Popup display:** shows the original signed value with a sign-aware label
  (e.g. `-25.3 MW (consuming)`).
- **Aggregate "Total power" metric in header:** sum of signed values, so
  charging cancels discharging — this is the conventional NEM net-generation
  view.

### 5.3 HTML in `facility_description`

The descriptions contain `<p>`, `<br/>`, `&#x27;` etc. We strip tags using
`html.parser.HTMLParser` (stdlib), decode entities, collapse whitespace, and
truncate to 280 chars + ellipsis for the popup.

### 5.4 Marker size scaling

Capacity ranges from ~6 MW (Christies Beach battery) to 2640 MW (Bayswater).
A linear scale makes small plants invisible; a raw log scale makes the
biggest plants too dominant. We use `sqrt(capacity_registered_total)`
mapped to a px range of `[6, 30]`. This is a common cartographic compromise
(area ≈ value).

### 5.5 Same `facility_name` but different `facility_code`

The data has duplicates by display name: `Broken Hill` appears as
`BHB` (battery), `BHILLGT` (distillate), `BROKENH` (solar). The state dict
is keyed on `facility_code`, not `facility_name`, so these stay separate.

### 5.6 Idempotent updates

Re-receiving an identical message for the same `facility_code` overwrites
the entry rather than appending. The map always reflects the **most recent**
observation per facility.

---

## 6. Dashboard Layout

```
┌────────────────────────────────────────────────────────────────────┐
│  Header: COMP5339 — NEM Live Dashboard                             │
│  Stats:  N facilities | Σ power MW | Σ emissions t | last update   │
├────────────────────┬───────────────────────────────────────────────┤
│  Controls (left)   │  Map (right)                                  │
│                    │                                               │
│  Display metric:   │   Plotly scatter_map, OSM tiles               │
│   ○ Power          │   • markers coloured by fueltech              │
│   ● Emissions      │   • size by capacity (sqrt scale)             │
│                    │   • hover shows compact summary               │
│  Region filter:    │   • click → popup card on right column        │
│   ☑ NSW1 ☑ VIC1   │                                               │
│   ☑ QLD1 ☑ SA1    │                                               │
│   ☑ TAS1          │                                               │
│                    │                                               │
│  Fueltech filter:  │                                               │
│   ☑ Coal ☑ Gas    │                                               │
│   ☑ Solar ☑ Wind  │                                               │
│   ☑ Hydro ☑ Batt  │                                               │
│   ☑ Other         │                                               │
└────────────────────┴───────────────────────────────────────────────┘
```

### Click-popup contents

```
🏭 Bayswater                       [×]
   Coal (Black) · NSW1

   Power now        1,715.4 MW
   Emissions now      128.9 t
   Registered cap   2,640.0 MW
   Utilisation        65.0 %

   Units (4)    BW01, BW02, BW03, BW04
   Last update  2025-10-01 00:00 AEST

   Bayswater Power Station is a bituminous (black)
   coal-powered thermal power station with four
   660 MW Tokyo Shibaura Electric steam-driven …
```

### Colour palette (fueltech → colour)

Inspired by but not identical to OpenElectricity's palette:

| Fueltech                                  | Colour      |
|-------------------------------------------|-------------|
| `coal_black`                              | `#1a1a1a`   |
| `coal_brown`                              | `#5a3a1a`   |
| `gas_ocgt` / `gas_ccgt` / `gas_recip` / `gas_steam` | `#f39c12` |
| `distillate`                              | `#c0392b`   |
| `solar_utility`                           | `#f1c40f`   |
| `wind`                                    | `#27ae60`   |
| `hydro`                                   | `#2980b9`   |
| `battery`                                 | `#8e44ad`   |
| `bioenergy_biomass` / `bioenergy_biogas`  | `#16a085`   |
| (anything else)                           | `#7f8c8d`   |

---

## 7. Extension Points

The prototype keeps these slots clearly marked so we can add features
without rewriting the core:

1. **`FUELTECH_COLOURS` dict** — add/edit colours in one place; cascades to
   legend automatically.
2. **`derive_primary_fueltech()`** — single function for the "which fueltech
   is this?" rule; easy to swap if the heuristic changes.
3. **`build_popup_card()`** — pure function from message → Dash component
   tree. Add fields (e.g. market price/demand for the optional Task 1) by
   editing this one function.
4. **`STATE` dict** — currently just `dict[facility_code, latest_msg]`.
   Can be upgraded to `dict[facility_code, list[msg]]` for history /
   sparklines without touching the rendering code.
5. **`get_state_snapshot()`** — the single point where the Dash side reads
   from the shared state. Easy place to plug in caching, filtering at
   read-time, or a database round-trip later.
6. **Topic subscription pattern** — currently `.../facility/#` wildcard.
   Adding a second topic (e.g. price/demand) is one line.
7. **DB integration (Task 4)** — `on_message` is the natural place to also
   persist to the database. Currently commented out; uncomment to wire in.

---

## 8. Known Limitations of the Prototype

- **No persistence yet.** State lives in memory; restarting the dashboard
  loses everything until the publisher re-sends. The plan is to back the
  state dict with the Task 4 database after the prototype is validated.
- **No history view.** Only the latest record per facility is kept.
- **No market price / demand layer.** Will be added if the teammate
  completes the optional Task 1 sub-task.
- **No reconnection backoff.** paho-mqtt's auto-reconnect is enabled with
  defaults; if the broker is flaky we may need to tune this.
- **Click popup is rendered in a fixed side panel**, not as an actual
  Mapbox popup tied to the marker. This is intentional — it keeps the
  popup readable on small screens and avoids Plotly's limited native popup
  API.
- **MQTT broker is the public HiveMQ test broker.** Anyone subscribed to
  the same topic prefix sees our messages. For grading this is fine; for
  any production-style deployment we would run a private Mosquitto.

---

## 9. File Layout

```
comp5339_task5/
├── PROJECT_DOC.md            ← this file
├── build_database.py         ← Task 4: builds energy.duckdb (run once)
├── db.py                     ← Task 4: persistence + integration module
├── dashboard_prototype.py    ← Task 5: runnable dashboard (now DB-aware)
├── requirements.txt          ← pinned deps for clean venv
├── data/
│   ├── nger_emissions_enriched.csv   ← Assignment-1 table snapshot
│   ├── cer_accredited_geo.csv        ← Assignment-1 table snapshot
│   └── abs_economy.csv               ← Assignment-1 table snapshot
└── energy.duckdb             ← generated by build_database.py (not committed)
```

Run order:

```
python build_database.py        # once, creates energy.duckdb (4 tables)
python dashboard_prototype.py   # starts subscriber + dashboard
```

---

## 10. Open Questions / Things To Confirm With Teammate

1. **Topic format.** Currently `.../facility/jza/tky` — is the suffix stable
   per run, or will it change every restart? If it changes, our wildcard
   subscription handles it; just want to confirm.
2. **Publish rate.** Brief says ≥ 0.1 s between messages and 60 s between
   API rounds. Are messages **republished** every round, or only on change?
   Affects whether `last update` jumps in big steps.
3. **Optional Task 1.** Market price and demand ARE being published
   (confirmed). Topic + schema still to be wired into the dashboard as a
   second subscription (planned next).
4. **Run order.** Should the dashboard be started **before** the publisher
   so it catches the first round? Or is the publisher buffered?
5. **Database (Task 4).** Decision: the **dashboard side writes**
   (subscriber persists). Rationale below in §12.

---

## 11. Changelog

| Date       | Change                                                                 |
|------------|------------------------------------------------------------------------|
| 2026-05-16 | Initial design doc + prototype skeleton.                               |
| 2026-05-16 | Task 4 added: build_database.py, db.py, DB-aware dashboard. §12 added. |
| 2026-05-23 | Iteration 2: generation chart, unit subtable, facility list. §13 added.|
| 2026-05-23 | Iteration 3: top filter bar, collapsible list, click-to-recentre. §14. |
| 2026-05-26 | Iteration 4: star schema (facility_dim + fact), FK, GEOMETRY. §15.    |

---

## 12. Task 4 — Database Schema & Assignment-1 Integration

> This section is written to be largely re-usable in the report's
> "Data Integration" answer.

### 12.1 Interpretation chosen

The brief says "Implement the schema (which was designed in Assignment 1)".
We adopt **interpretation A: reuse the existing Assignment-1 database and
integrate the live stream into it**, rather than designing a brand-new
schema. This directly serves the report question *"Explain how you
integrate the MQTT messages with your existing data from Assignment 1"*
and respects the brief's emphasis on the word *existing*.

### 12.2 What is in energy.duckdb

`build_database.py` creates one DuckDB file with four tables:

| Table | Origin | Role |
|-------|--------|------|
| `nger_emissions_enriched` | Assignment 1 (CSV snapshot) | Historical emissions & generation, 2014-15…2023-24 (5,938 rows). Read-only. |
| `cer_accredited_geo` | Assignment 1 (CSV snapshot) | Geocoded CER accredited renewable stations (57 rows). Read-only. |
| `abs_economy` | Assignment 1 (CSV snapshot) | ABS economy/industry by region & year (1,150 rows, 4 geographic levels). Read-only. |
| `live_observations` | **Assignment 2 (this project)** | One row per facility per `event_time` from the MQTT stream. Written at runtime. |

The three reference tables are loaded from CSV snapshots taken from the
Assignment-1 DuckDB (`energy.duckdb` was not retained, so the CSV exports
are the source of truth). The build is idempotent: reference tables are
dropped/recreated each run; `live_observations` is preserved across
rebuilds unless `--reset-live` is passed.

> **Honest scope note for the report:** the `abs_economy` snapshot is the
> full 1,150-row table (all four geographic granularities incl.
> `geographic_level`). Only the `geographic_level='state'` subset
> (~90 rows: 9 categories × 10 years) is exercised by the dashboard
> integration, because facilities join to state-level economic context.

### 12.3 `live_observations` schema and why

```sql
CREATE TABLE live_observations (
    facility_code              VARCHAR,
    facility_name              VARCHAR,
    network_id                 VARCHAR,
    network_region             VARCHAR,   -- raw NEM code, e.g. 'NSW1'
    state                      VARCHAR,   -- derived join key, e.g. 'NSW'
    event_time                 TIMESTAMP, -- the data's own timestamp
    power_mw                   DOUBLE,
    emissions_t                DOUBLE,
    primary_fueltech           VARCHAR,
    fueltech_summary           VARCHAR,
    capacity_registered_total  DOUBLE,
    unit_count                 INTEGER,
    latitude                   DOUBLE,
    longitude                  DOUBLE,
    ingested_at                TIMESTAMP, -- wall clock at write
    PRIMARY KEY (facility_code, event_time)
);
```

Design decisions:

- **PK `(facility_code, event_time)`** makes the stream idempotent —
  the publisher re-sends every 60 s round, and `INSERT OR REPLACE`
  keyed on this pair guarantees one row per facility per event time, no
  duplicates, no append-bloat.
- **`state` is materialised** at write time (not computed at query time)
  so every join into the Assignment-1 tables is a simple equality.
- **`event_time` vs `ingested_at`** are kept separate so stream lag is
  measurable (the data is dated 2025-10-01 but ingested in May 2026).

### 12.4 The integration key: NEM region → state

This is the crux of the integration and the main "challenge encountered".

The MQTT stream tags each record with a **NEM region code**:
`NSW1, VIC1, QLD1, SA1, TAS1`. The Assignment-1 tables key on a
**state code**: `NSW, VIC, QLD, SA, TAS, WA, NT, ACT`. They are not the
same vocabulary. We bridge them with a deterministic rule —
**strip the trailing digit**: `NSW1 → NSW`, `SA1 → SA`. Implemented once
in `db.region_to_state()` and materialised into `live_observations.state`.

### 12.5 The second challenge: fuzzy facility-name matching

The stream's `facility_name` is short and mixed-case (`"Bayswater"`),
while `nger_emissions_enriched.facility_name` is the full upper-case
name (`"BAYSWATER POWER STATION"`). Exact matching fails for almost
every facility. We resolve this with a **normalised containment match**:
uppercase both sides and match where the NGER name contains the stream
name (or vice-versa), narrowed by `state` to avoid cross-state false
positives, restricted to actual facility rows (`type IN ('F','FA')`).

This is imperfect by construction (a short stream name could match
multiple NGER facilities; some facilities have no NGER history at all —
e.g. new batteries). The dashboard shows the matched NGER name explicitly
so the user can judge the match, and shows "No Assignment-1 reference
match" when nothing is found. This honest surfacing of match quality is
itself part of the integration story for the report.

### 12.6 How integration is surfaced

When a marker is clicked, `db.get_integration()` runs two joins and the
popup shows an **"Integrated with Assignment 1"** block:

- **NGER history** (join on fuzzy `facility_name` + `state`): latest
  financial year's total emissions, emission intensity, generation, and
  how many financial years of history exist for that facility.
- **State economy** (join on `state`, `geographic_level='state'`,
  latest year): number of businesses and persons employed in that
  facility's state.

So a live battery reporting `0 MW` at midnight is shown alongside its
decade of NGER emissions and its state's economic backdrop — turning a
single stream tick into an integrated view across all of Assignment 1.

### 12.7 Concurrency model

DuckDB permits one read-write connection per process and its connections
are not thread-safe to share. The MQTT thread writes; the Dash callback
thread reads (integration queries). Both go through **one module-level
connection guarded by a single lock** in `db.py`. At ~10 messages/s the
lock is never a bottleneck. A DB failure in `persist_observation()` is
caught and logged so it can never kill the subscriber loop; if
`energy.duckdb` is absent the dashboard degrades gracefully to live-map-
only (verified by regression test).

---

## 13. Iteration 2 — Generation chart, unit table, facility list

Three additions on top of the Task 4 baseline. All are wired into the
existing extension points; no architectural change.

### 13.1 Live generation chart (`build_generation_chart`)

Each time the popup re-renders, `db.get_facility_history()` pulls the
last 120 observations (≈10 hours at the 5-min cadence) for the selected
facility from `live_observations` and draws a compact area chart of
`power_mw` over `event_time`. Because the DB is the source of truth, the
chart **survives dashboard restarts** and extends naturally as new MQTT
messages stream in.

This is the user-facing payoff for persisting the stream in Task 4.

### 13.2 Unit subtable (`build_unit_table`)

The MQTT message already carries `unit_details` (a list of per-unit
records). The popup now renders this as a small four-column table
(unit code, fueltech, registered capacity, dispatch type), one row per
unit. Directly answers the report question *"how did you handle that
some power facilities consist of several facility units?"* — at the
data-acquisition layer we keep the per-unit detail intact in the
message and surface it on demand in the UI, rather than collapsing it
at ingest time.

### 13.3 Facility list (`refresh_facility_list`)

A left-side `dash_table.DataTable` lists all received facilities with
five columns: name, region, tech, capacity, current power. Sorted by
current power descending by default. Selecting a row updates the same
`selected-facility` Store as clicking a marker, so the popup is driven
from one source regardless of which control the user touches. The list
reuses the same `build_dataframe()` filter pipeline as the map, so the
region/fueltech filters affect both views consistently.

### 13.4 Layout change

The left side gained one column; the page is now:

```
controls (200) | facility list (280) | map (flex) | popup (320)
```

Total fixed: 800 px + flex map. Works on a 1366 px laptop; comfortable
above 1440 px.

---

## 14. Iteration 3 — Top filter bar, collapsible list, click-to-recentre

UX polish, no architectural change.

### 14.1 Layout: filters moved to a top bar

The original 200-px left column of filters was eating screen real
estate. It is replaced by a horizontal bar between the header and the
map area, with three controls:

- **Show** (Power / Emissions) — inline radio
- **Region** — `dcc.Dropdown(multi=True)` with the five NEM regions
- **Tech** — `dcc.Dropdown(multi=True)` with the fueltech groups

Both dropdowns are `clearable=False` so the user cannot accidentally
end up with an empty filter and a blank map.

### 14.2 Collapsible facility list

The list panel is now collapsible. A small "« Hide list" button at the
top toggles between two states stored in `dcc.Store(id="list-open")`:

| State  | Panel width | Content    | Button label |
|--------|-------------|------------|--------------|
| Open   | 290 px      | visible    | "« Hide list"|
| Closed | 32 px       | `display:none` | "»"      |

A CSS `transition: width 0.2s` keeps the collapse animation smooth.
Closed state still shows the toggle so it can be reopened.

### 14.3 Click-to-recentre + selection highlight

Selecting a facility (via marker or list row) now recentres the map
on that facility's lat/lon and draws a translucent red halo over its
marker. The implementation relies on Plotly's `uirevision` mechanism:

- `uirevision = f"sel-{selected_code or 'none'}"`
- During poll refreshes with the same selection, `uirevision` is
  unchanged, so Plotly preserves whatever pan/zoom the user has
  manually set.
- When the selection changes, `uirevision` changes, and Plotly applies
  the new `map.center` we set in the figure layout.

The user requested "zoom unchanged", so we do not override zoom on
selection — only the centre moves. The red halo (a second
`Scattermap` trace at +24 px size, opacity 0.35) compensates for the
fact that at the default continent-wide zoom (4.2) a recentre of a few
hundred kilometres is visually subtle.

Scattermap markers do not support a CSS-style border, so the halo is
implemented as an overlaid disc rather than a stroked ring.

---

## 15. Iteration 4 — Star schema, foreign key, GEOMETRY

This iteration addresses three specific gaps surfaced in the
Assignment 1 feedback:

1. *"Did not specify primary keys when creating tables in the code."*
2. *"Did not specify foreign keys when creating tables in the code."*
3. *"Did not convert latitude and longitude into geo-spatial format
   (i.e. GEOMETRY) using DuckDB spatial extension."*

### 15.1 The original (fact-only) layout

Iteration 1 used a single denormalised table:

```sql
CREATE TABLE live_observations (
    facility_code, facility_name, network_id, network_region, state,
    event_time, power_mw, emissions_t,
    primary_fueltech, fueltech_summary,
    capacity_registered_total, unit_count,
    latitude, longitude,
    ingested_at,
    PRIMARY KEY (facility_code, event_time)
);
```

It had a PK but no FK, no GEOMETRY, and conflated slow-changing
facility metadata (name, region, capacity, location) with the
high-volume measurement stream (power_mw, emissions_t per 5 min).
Every observation duplicated the metadata.

### 15.2 The new star schema

Iteration 4 separates dimensions from facts.

```sql
-- Dimension: one row per facility, slow-changing.
CREATE TABLE facility_dim (
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
    geom                       GEOMETRY,          -- ST_Point(lon, lat)
    first_seen                 TIMESTAMP,
    last_seen                  TIMESTAMP
);

-- Fact: one row per (facility × event_time), append-mostly.
CREATE TABLE live_observations (
    facility_code   VARCHAR     NOT NULL,
    event_time      TIMESTAMP   NOT NULL,
    power_mw        DOUBLE,
    emissions_t     DOUBLE,
    ingested_at     TIMESTAMP,
    PRIMARY KEY (facility_code, event_time),
    FOREIGN KEY (facility_code) REFERENCES facility_dim(facility_code)
);
```

### 15.3 Why a star (not a single wide table)

- **Normalisation of slow-changing data.** A facility's name,
  fueltech, and capacity are essentially static across the stream;
  storing them once in `facility_dim` removes ~12 redundant columns
  per observation.
- **Enforced referential integrity.** The FK means orphan facts are
  impossible — DuckDB raises `ConstraintException` if a fact is
  inserted before its facility dimension exists. Verified by
  regression test (an INSERT with `facility_code='ORPHAN'` fails).
- **Lifecycle tracking.** `first_seen` / `last_seen` on the dim
  capture when each facility entered and was last observed in the
  stream — useful for "stale facility" detection in the dashboard.
- **Independence of the two write rates.** The dim is upserted only
  when metadata changes; the fact grows linearly with the stream.

### 15.4 The write path (`db.persist_observation`)

Every MQTT message triggers a two-statement transaction inside the
existing `_DB_LOCK`:

```sql
-- 1. Upsert into the dim (geom built server-side).
INSERT INTO facility_dim (..., geom, first_seen, last_seen)
VALUES (..., ST_Point(longitude, latitude), now, now)
ON CONFLICT (facility_code) DO UPDATE SET
    facility_name = EXCLUDED.facility_name,
    ...
    last_seen     = EXCLUDED.last_seen;
-- (first_seen is NOT in the SET list — it is preserved.)

-- 2. Insert the fact; FK is now guaranteed.
INSERT OR REPLACE INTO live_observations
  (facility_code, event_time, power_mw, emissions_t, ingested_at)
VALUES (?, ?, ?, ?, ?);
```

`ST_Point(longitude, latitude)` is the OGC standard ordering (X = lon,
Y = lat). The `geom` column is guarded against NULL lat/lon via a
`CASE WHEN` so a malformed message does not blow up the upsert.

### 15.5 Spatial extension lifecycle

`build_database.py` runs `INSTALL spatial; LOAD spatial;` on the build
connection so the extension is downloaded once and the GEOMETRY column
can be created. `db.py` runs `LOAD spatial;` on every dashboard
connection so `ST_Point` and any future `ST_*` queries work without
the dashboard needing to know about the extension.

If the user's machine has no internet on first run, `INSTALL` fails
loudly with a clear error from `build_database.py`. After one
successful install DuckDB caches the extension locally and offline
runs work.

### 15.6 What this enables (future, not yet built)

The GEOMETRY column is not just rubric compliance — it enables real
spatial analytics inside DuckDB itself:

- Facility clusters within N km of an emissions hotspot:
  `ST_DWithin(geom, ST_Point(lon, lat), 50000)`
- "Facilities within state polygon" if we ever load ABS SA4 boundary
  geometry alongside `abs_economy`.

These are deliberately not implemented now; the schema just leaves
the door open.

### 15.7 Migration

`build_database.py` auto-detects the old wide-table layout by checking
for legacy columns (`facility_name`, `latitude`, etc.) inside
`live_observations`. If found, both live tables are dropped and
recreated under the new schema, and a one-line migration notice is
printed. This means: a user re-running the script after pulling this
iteration loses their accumulated stream history once (necessary, the
old schema cannot be in-place converted), but never has to think
about the migration manually.
