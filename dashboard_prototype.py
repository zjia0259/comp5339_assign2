"""
COMP5339 Assignment 2 — Task 5: Real-time NEM facilities dashboard.


Run:
    python dashboard_prototype.py

Then open http://127.0.0.1:8050 in a browser. Wait a few seconds for the
first MQTT messages to arrive — the map starts empty and fills in as
messages stream in.

Tested with: paho-mqtt 2.x, dash 2.x or 3.x, plotly 5.24+.
"""

from __future__ import annotations

import json
import threading
from copy import deepcopy
from datetime import datetime
from html.parser import HTMLParser
from typing import Any

import dash
import pandas as pd
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import plotly.graph_objects as go
from dash import Input, Output, State, dash_table, dcc, html



# Task 4 — database persistence + Assignment-1 integration.
# Isolated in db.py so the dashboard core stays unchanged; if energy.duckdb
# is missing the dashboard still runs (live map only, no DB features).
import db as task4_db

_DB_ENABLED = task4_db.db_available()
if not _DB_ENABLED:
    print("[main] energy.duckdb not found — running WITHOUT Task 4 DB "
          "features. Run `python build_database.py` to enable them.")


# ---------------------------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------------------------
# Keep all tunables in one block so the teammate / tutor can find them.

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "comp5339/electricity/facility/jza/tky"  # wildcard — see PROJECT_DOC §4
# Optional Task 1 (price + regional demand). The exact topic the teammate's
# publisher uses for the market feed is not yet confirmed; we subscribe to
# this wildcard AND content-route inside _on_message, so the dashboard is
# robust to either topic choice as long as the payload carries the
# `price_aud_mwh` / `demand_mw_region` keys.
MQTT_TOPIC_MARKET = "comp5339/electricity/market/jza/tky"
CLIENT_ID = "comp5339_dashboard_subscriber"

# How often the dashboard polls the shared state and re-renders. The
# prototype is intentionally conservative; if the publisher is producing
# 10+ messages/sec, drop this to 1000 ms.
REFRESH_INTERVAL_MS = 2000

# Map starting viewport — centred roughly on south-east Australia (NEM area).
MAP_CENTER = {"lat": -32.5, "lon": 145.0}
MAP_ZOOM = 4.2
# Zoom level applied when a facility is selected. Changing zoom (not only
# center) is what reliably makes MapLibre recentre — see build_map_figure.
MAP_SELECTED_ZOOM = 6.5

# Visual sizing for markers (see PROJECT_DOC §5.4).
MARKER_MIN_PX = 6
MARKER_MAX_PX = 30

# All five NEM regions. Used for the region filter checklist.
NEM_REGIONS = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]

# Fueltech → colour. Extension point: edit this dict in one place and the
# map legend updates automatically.
FUELTECH_COLOURS = {
    "coal_black":         "#1a1a1a",
    "coal_brown":         "#5a3a1a",
    "gas_ocgt":           "#f39c12",
    "gas_ccgt":           "#e67e22",
    "gas_recip":          "#d68910",
    "gas_steam":          "#b9770e",
    "distillate":         "#c0392b",
    "solar_utility":      "#f1c40f",
    "solar_rooftop":      "#f7dc6f",
    "wind":               "#27ae60",
    "hydro":              "#2980b9",
    "pumps":              "#5dade2",
    "battery":            "#8e44ad",
    "bioenergy_biomass":  "#16a085",
    "bioenergy_biogas":   "#1abc9c",
}
DEFAULT_COLOUR = "#7f8c8d"

# Human-readable labels for the fueltech filter checklist.
FUELTECH_GROUPS = {
    "Coal":    ["coal_black", "coal_brown"],
    "Gas":     ["gas_ocgt", "gas_ccgt", "gas_recip", "gas_steam"],
    "Solar":   ["solar_utility", "solar_rooftop"],
    "Wind":    ["wind"],
    "Hydro":   ["hydro", "pumps"],
    "Battery": ["battery"],
    "Bio":     ["bioenergy_biomass", "bioenergy_biogas"],
    "Other":   ["distillate"],  # plus anything not listed above
}


# ---------------------------------------------------------------------------
# 2. HELPER FUNCTIONS
# ---------------------------------------------------------------------------

class _HTMLStripper(HTMLParser):
    """Tiny HTML → text converter, stdlib only."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._chunks: list[str] = []

    def handle_data(self, data: str) -> None:
        self._chunks.append(data)

    def handle_starttag(self, tag: str, attrs: Any) -> None:  # noqa: ARG002
        if tag in ("br", "p"):
            self._chunks.append(" ")

    def text(self) -> str:
        return " ".join("".join(self._chunks).split())


def strip_html(s: str | None, max_chars: int = 280) -> str:
    """Strip HTML tags from a description and truncate."""
    if not s:
        return ""
    stripper = _HTMLStripper()
    try:
        stripper.feed(s)
    except Exception:
        return ""
    txt = stripper.text()
    if len(txt) > max_chars:
        txt = txt[: max_chars - 1].rstrip() + "…"
    return txt


def derive_primary_fueltech(fueltech_summary: str | None) -> str:
    """Pick a primary fueltech from a pipe-separated summary.

    Skips the derivative '_charging' / '_discharging' battery views so
    that 'battery|battery_charging|battery_discharging|solar_utility'
    resolves to 'battery'.

    See PROJECT_DOC §5.1 for the rationale.
    """
    if not fueltech_summary:
        return "unknown"
    parts = [p for p in fueltech_summary.split("|") if p]
    if not parts:
        return "unknown"
    for p in parts:
        if not p.endswith("_charging") and not p.endswith("_discharging"):
            return p
    return parts[0]


def fueltech_to_colour(ft: str) -> str:
    return FUELTECH_COLOURS.get(ft, DEFAULT_COLOUR)


def fueltech_group(ft: str) -> str:
    """Map a raw fueltech to its display group (for filtering)."""
    for group, members in FUELTECH_GROUPS.items():
        if ft in members:
            return group
    return "Other"


def marker_size(capacity_mw: float | None) -> float:
    """sqrt-scaled marker size, clamped to [MIN, MAX]."""
    if capacity_mw is None or capacity_mw <= 0:
        return MARKER_MIN_PX
    raw = (capacity_mw ** 0.5) * 1.2
    return max(MARKER_MIN_PX, min(MARKER_MAX_PX, raw))


def format_event_time(s: str | None) -> str:
    """Render an ISO 8601 timestamp as 'YYYY-MM-DD HH:MM TZ'."""
    if not s:
        return "—"
    try:
        dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%d %H:%M %Z").strip()
    except ValueError:
        return s


# ---------------------------------------------------------------------------
# 3. SHARED STATE
# ---------------------------------------------------------------------------
# A single dict keyed by facility_code holds the latest message per
# facility. The MQTT thread writes; the Dash thread reads.

_STATE: dict[str, dict] = {}
_STATE_LOCK = threading.Lock()
_LAST_MESSAGE_AT: datetime | None = None

# Per-region market data (optional Task 1). Keyed by network_region
# (e.g. "NSW1") -> last received {price_aud_mwh, demand_mw_region,
# event_time}. Lives next to _STATE, written by the MQTT thread and
# read by the Dash callbacks. Same lock for simplicity — the lock
# protects "the in-memory snapshot of the stream" as a whole.
_MARKET_STATE: dict[str, dict] = {}


def update_market_state(message: dict) -> None:
    """Thread-safe write into the per-region market state."""
    global _LAST_MESSAGE_AT
    region = message.get("network_region")
    if not region:
        return
    with _STATE_LOCK:
        _MARKET_STATE[region] = message
        _LAST_MESSAGE_AT = datetime.now()


def get_market_snapshot() -> dict[str, dict]:
    """Return a copy of the per-region market state for Dash to read."""
    with _STATE_LOCK:
        return deepcopy(_MARKET_STATE)


def update_state(message: dict) -> None:
    """Thread-safe write into the shared state."""
    global _LAST_MESSAGE_AT
    facility_code = message.get("facility_code")
    if not facility_code:
        return
    with _STATE_LOCK:
        _STATE[facility_code] = message
        _LAST_MESSAGE_AT = datetime.now()


def get_state_snapshot() -> tuple[dict[str, dict], datetime | None]:
    """Return a deep copy of the state for safe read in the Dash thread.

    Extension point: this is the single read path. If we later swap the
    in-memory dict for a database query, only this function changes.
    """
    with _STATE_LOCK:
        return deepcopy(_STATE), _LAST_MESSAGE_AT


# ---------------------------------------------------------------------------
# 4. MQTT SUBSCRIBER (background thread)
# ---------------------------------------------------------------------------

def _on_connect(client, userdata, flags, reason_code, properties):  # noqa: ARG001
    if reason_code == 0:
        print(f"[MQTT] Connected. Subscribing to {MQTT_TOPIC!r} "
              f"and {MQTT_TOPIC_MARKET!r}")
        # Two wildcard subscriptions. Even if the publisher uses an
        # unexpected topic, the content-routing in _on_message will
        # still classify the message correctly.
        client.subscribe(MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC_MARKET)
    else:
        print(f"[MQTT] Connect failed: {reason_code}")


def _classify(payload: dict) -> str:
    """Decide whether a received payload is a facility or market message.

    Content-based routing makes us resilient to the exact topic the
    publisher uses for the market feed. The classification is:

      • has 'facility_code'                     -> 'facility'
      • has 'price_aud_mwh' or 'demand_mw_region' (and a region)
                                                -> 'market'
      • otherwise                               -> 'unknown'
    """
    if payload.get("facility_code"):
        return "facility"
    if payload.get("network_region") and (
        "price_aud_mwh" in payload or "demand_mw_region" in payload
    ):
        return "market"
    return "unknown"


def _on_message(client, userdata, msg):  # noqa: ARG001
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        print(f"[MQTT] Bad payload on {msg.topic}: {e}")
        return

    kind = _classify(payload)
    if kind == "facility":
        update_state(payload)
        # Optional Task 1: the spot price and regional demand travel
        # INSIDE each facility message, under the keys `price_aud_per_mwh`
        # / `demand_mw`. Build a region-level market record from them and
        # feed the same market state + DB table. We map the incoming names
        # to the canonical internal names (price_aud_mwh / demand_mw_region)
        # so nothing downstream (tiles, popup, DB) needs to change.
        region = payload.get("network_region")
        if region and ("price_aud_per_mwh" in payload or "demand_mw" in payload):
            market_msg = {
                "network_region": region,
                "event_time": payload.get("event_time"),
                "price_aud_mwh": payload.get("price_aud_per_mwh"),
                "demand_mw_region": payload.get("demand_mw_region"),
            }
            update_market_state(market_msg)
            if _DB_ENABLED:
                task4_db.persist_market(market_msg)
        # Task 4: persist to energy.duckdb in parallel with the in-memory
        # state. Wrapped so a DB failure can never kill the subscriber loop.
        if _DB_ENABLED:
            task4_db.persist_observation(payload)
    elif kind == "market":
        update_market_state(payload)
        if _DB_ENABLED:
            task4_db.persist_market(payload)
    else:
        # Drop silently — unknown payload shape. Could be a malformed
        # message or a future message type. We just don't surface it.
        return


def start_subscriber() -> mqtt.Client:
    """Start the MQTT subscriber on a background thread and return the client."""
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=CLIENT_ID,
    )
    client.on_connect = _on_connect
    client.on_message = _on_message
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()  # non-blocking; runs in its own thread
    return client


# ---------------------------------------------------------------------------
# 5. DATA → FIGURE
# ---------------------------------------------------------------------------

def build_dataframe(
    state: dict[str, dict],
    regions: list[str],
    fueltech_groups: list[str],
) -> pd.DataFrame:
    """Flatten the state dict into a DataFrame, applying filters.

    Extension point: add more derived columns here (e.g. utilisation,
    market price) and they'll be available in the hover and popup.
    """
    rows = []
    for msg in state.values():
        primary_ft = derive_primary_fueltech(msg.get("fueltech_summary"))
        group = fueltech_group(primary_ft)
        region = msg.get("network_region", "")

        # Apply filters.
        if region not in regions:
            continue
        if group not in fueltech_groups:
            continue

        cap = msg.get("capacity_registered_total") or 0.0
        power = msg.get("power_mw") or 0.0
        emissions = msg.get("emissions_t") or 0.0

        rows.append({
            "facility_code": msg.get("facility_code"),
            "facility_name": msg.get("facility_name"),
            "lat": msg.get("latitude"),
            "lon": msg.get("longitude"),
            "region": region,
            "fueltech": primary_ft,
            "fueltech_group": group,
            "colour": fueltech_to_colour(primary_ft),
            "capacity_mw": cap,
            "power_mw": power,
            "emissions_t": emissions,
            "abs_power_mw": abs(power),
            "size": marker_size(cap),
            "event_time": msg.get("event_time"),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.dropna(subset=["lat", "lon"])
    return df


def build_map_figure(df: pd.DataFrame, metric: str,
                     selected_code: str | None = None) -> go.Figure:
    """Build the scatter_map figure. `metric` ∈ {'power', 'emissions'}.

    If `selected_code` is given and matches a row in `df`, the map is
    recentred on that facility and a highlight ring is drawn over it.
    The map's `uirevision` is keyed on `selected_code`, so:
      - polling refreshes with the SAME selection preserve user
        pan/zoom (uirevision unchanged → Plotly keeps camera state)
      - a NEW selection changes uirevision → Plotly applies the
        new `center` we set in the layout
    """
    fig = go.Figure()

    # The uirevision token — see docstring. We embed selected_code so
    # any selection change triggers Plotly to honour our new `center`.
    uirev = f"sel-{selected_code or 'none'}"

    if df.empty:
        fig.update_layout(
            map={"style": "open-street-map", "center": MAP_CENTER, "zoom": MAP_ZOOM},
            margin={"l": 0, "r": 0, "t": 0, "b": 0},
            height=720,
            uirevision=uirev,
            annotations=[{
                "text": "Waiting for MQTT messages…",
                "xref": "paper", "yref": "paper",
                "x": 0.5, "y": 0.5,
                "showarrow": False,
                "font": {"size": 18, "color": "#888"},
            }],
        )
        return fig

    if metric == "power":
        value = df["power_mw"]
        unit = "MW"
    else:
        value = df["emissions_t"]
        unit = "t"

    hover = [
        f"<b>{r.facility_name}</b><br>"
        f"{r.fueltech} · {r.region}<br>"
        f"Power: {r.power_mw:,.1f} MW<br>"
        f"Emissions: {r.emissions_t:,.2f} t<br>"
        f"Capacity: {r.capacity_mw:,.0f} MW<br>"
        f"<i>click for details</i>"
        for r in df.itertuples()
    ]

    fig.add_trace(
        go.Scattermap(
            lat=df["lat"],
            lon=df["lon"],
            mode="markers",
            marker={
                "size": df["size"],
                "color": df["colour"],
                "opacity": 0.85,
            },
            text=hover,
            hoverinfo="text",
            customdata=df[["facility_code"]].values,
            name="facilities",
        )
    )

    # Recentre + highlight if a facility is selected.
    # Recentre + highlight if a facility is selected.
    center = MAP_CENTER
    zoom = MAP_ZOOM
    if selected_code:
        sel_rows = df[df["facility_code"] == selected_code]
        if not sel_rows.empty:
            r = sel_rows.iloc[0]
            center = {"lat": float(r["lat"]), "lon": float(r["lon"])}
            # Zoom in on selection. Changing zoom (not just center) is
            # what reliably forces MapLibre to fly to the new location;
            # with zoom held constant, a uirevision change often fails
            # to reapply `center`.
            zoom = MAP_SELECTED_ZOOM
            # Highlight halo: a translucent red disc drawn ON TOP of
            # the main marker (so it shows even on a black coal point)
            # at slightly larger size. Reads as "this one is selected".
            fig.add_trace(
                go.Scattermap(
                    lat=[r["lat"]],
                    lon=[r["lon"]],
                    mode="markers",
                    marker={
                        "size": float(r["size"]) + 24,
                        "color": "#ff4757",
                        "opacity": 0.35,
                    },
                    hoverinfo="skip",
                    showlegend=False,
                    name="selection-highlight",
                )
            )

    fig.update_layout(
        map={"style": "open-street-map", "center": center, "zoom": zoom},
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        height=720,
        showlegend=False,
        uirevision=uirev,
    )

    # Show the active metric in a small overlay so users know what colour
    # / value they're looking at.
    total = value.sum()
    fig.add_annotation(
        xref="paper", yref="paper", x=0.01, y=0.99,
        xanchor="left", yanchor="top",
        text=f"<b>Showing {metric}</b> · Σ = {total:,.1f} {unit}",
        showarrow=False,
        bgcolor="rgba(255,255,255,0.85)",
        bordercolor="#999", borderwidth=1, borderpad=6,
        font={"size": 12},
    )
    return fig


# ---------------------------------------------------------------------------
# 6. POPUP CARD
# ---------------------------------------------------------------------------

def _popup_market_rows(network_region: str | None, row) -> list:
    """Return 0–2 popup rows showing the region's current price + demand.

    `row` is the same `row(label, value)` factory used inside
    build_popup_card; passing it in lets the rows look identical to the
    rest of the popup with no duplication of style. We read from the
    in-memory MARKET_STATE so this is O(1) and never hits the DB.
    """
    if not network_region:
        return []
    snap = get_market_snapshot()
    m = snap.get(network_region)
    if not m:
        # No market data received yet for this region — show a
        # dimmed-out placeholder pair so the popup still tells the
        # user there IS a market block, just no data yet.
        return [
            row(f"{network_region} price", "—"),
            row(f"{network_region} demand", "—"),
        ]
    price = m.get("price_aud_mwh")
    demand = m.get("demand_mw_region")
    price_s = f"${price:,.2f}/MWh" if price is not None else "—"
    demand_s = f"{demand:,.0f} MW" if demand is not None else "—"
    return [
        row(f"{network_region} price", price_s),
        row(f"{network_region} demand", demand_s),
    ]


def build_popup_card(msg: dict | None) -> list:
    """Construct the right-hand popup component tree.

    Extension point: add fields here (market price, demand, history
    sparkline, per-unit table) — this is the single source of truth for
    what the click-popup looks like.
    """
    if not msg:
        return [
            html.Div(
                "Click a marker on the map to see facility details.",
                style={"color": "#888", "fontStyle": "italic"},
            ),
        ]

    primary_ft = derive_primary_fueltech(msg.get("fueltech_summary"))
    cap = msg.get("capacity_registered_total") or 0.0
    power = msg.get("power_mw") or 0.0
    emissions = msg.get("emissions_t") or 0.0
    utilisation = (abs(power) / cap * 100.0) if cap > 0 else 0.0

    power_label = f"{power:,.1f} MW"
    if power < 0:
        power_label += " (consuming)"

    unit_codes = msg.get("unit_codes", "").replace("|", ", ")
    unit_count = msg.get("unit_count", 0)
    desc = strip_html(msg.get("facility_description"))

    def row(label: str, value: str) -> html.Div:
        return html.Div(
            [
                html.Span(label, style={"color": "#666", "fontSize": "12px"}),
                html.Span(value, style={"fontWeight": 600, "fontSize": "14px"}),
            ],
            style={
                "display": "flex",
                "justifyContent": "space-between",
                "padding": "4px 0",
                "borderBottom": "1px solid #eee",
            },
        )

    return [
        html.Div(
            [
                html.Div(
                    msg.get("facility_name", "—"),
                    style={"fontSize": "20px", "fontWeight": 700},
                ),
                html.Div(
                    f"{primary_ft} · {msg.get('network_region', '')}",
                    style={
                        "color": fueltech_to_colour(primary_ft),
                        "fontSize": "13px",
                        "marginBottom": "12px",
                    },
                ),
            ]
        ),
        row("Power now", power_label),
        row("Emissions now", f"{emissions:,.2f} t"),
        row("Registered cap.", f"{cap:,.0f} MW"),
        row("Utilisation", f"{utilisation:.1f} %"),
        row("Units", f"{unit_count} ({unit_codes})"),
        row("Last update", format_event_time(msg.get("event_time"))),
        # Optional Task 1: market context for this facility's region.
        # Pulled live from the in-memory market state (no DB hit per
        # marker click). Falls through to "—" if we have not yet
        # received any market message for this region.
        *_popup_market_rows(msg.get("network_region"), row),
        # Live generation chart (Need 3): pulls history from DuckDB.
        *build_generation_chart(msg.get("facility_code")),
        # Unit subtable (Need 1): expand `unit_details` if present.
        *build_unit_table(msg.get("unit_details")),
        html.Div(
            desc or "(no description)",
            style={
                "marginTop": "12px",
                "fontSize": "12px",
                "color": "#444",
                "lineHeight": "1.4",
            },
        ),
        # Task 4: integration with Assignment-1 reference data.
        *build_integration_section(
            msg.get("facility_name"), msg.get("network_region")
        ),
    ]


def build_generation_chart(facility_code: str | None) -> list:
    """Live mini chart of power_mw over event_time, fed by live_observations.

    Need 3: dynamic generation chart. Each refresh re-queries the DB so
    the chart extends as new MQTT messages arrive. Falls back to a
    placeholder if the DB is disabled or has < 2 points.
    """
    if not _DB_ENABLED or not facility_code:
        return []
    history = task4_db.get_facility_history(facility_code, limit=120)
    if len(history) < 2:
        return [
            html.Div(
                f"Generation history: collecting… ({len(history)} point so far)",
                style={"marginTop": "12px", "fontSize": "11px",
                       "color": "#999", "fontStyle": "italic"},
            )
        ]

    xs = [r[0] for r in history]
    ys_power = [r[1] for r in history]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=xs, y=ys_power, mode="lines",
            line={"color": "#2c3e50", "width": 1.6},
            fill="tozeroy", fillcolor="rgba(44,62,80,0.15)",
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>%{y:.1f} MW<extra></extra>",
        )
    )
    fig.update_layout(
        height=140,
        margin={"l": 4, "r": 4, "t": 4, "b": 4},
        xaxis={"showgrid": False, "tickfont": {"size": 9}},
        yaxis={"title": "MW", "tickfont": {"size": 9},
               "title_font": {"size": 10}, "gridcolor": "#eee"},
        plot_bgcolor="#fff",
        showlegend=False,
    )

    return [
        html.Div(
            f"Generation history ({len(history)} obs)",
            style={"marginTop": "14px", "fontSize": "12px",
                   "fontWeight": 600, "color": "#444"},
        ),
        dcc.Graph(
            figure=fig,
            config={"displayModeBar": False},
            style={"height": "140px", "marginTop": "4px"},
        ),
    ]


def build_unit_table(unit_details: list | None) -> list:
    """Render the facility's units as a compact table (Need 1).

    Mirrors the OpenElectricity style: one row per unit with code,
    fueltech, registered MW, and dispatch type. Handles the
    'a facility has several units' point that the report asks about.
    """
    if not unit_details:
        return []

    header = html.Tr([
        html.Th("Unit",     style={"textAlign": "left",  "fontSize": "10px", "color": "#777", "padding": "2px 4px"}),
        html.Th("Fueltech", style={"textAlign": "left",  "fontSize": "10px", "color": "#777", "padding": "2px 4px"}),
        html.Th("Cap.",     style={"textAlign": "right", "fontSize": "10px", "color": "#777", "padding": "2px 4px"}),
        html.Th("Type",     style={"textAlign": "left",  "fontSize": "10px", "color": "#777", "padding": "2px 4px"}),
    ])
    rows = [header]
    for u in unit_details:
        cap = u.get("capacity_registered")
        rows.append(html.Tr([
            html.Td(u.get("unit_code_display") or u.get("unit_code") or "—",
                    style={"fontSize": "11px", "padding": "2px 4px",
                           "fontFamily": "monospace"}),
            html.Td(u.get("fueltech_id") or "—",
                    style={"fontSize": "11px", "padding": "2px 4px",
                           "color": fueltech_to_colour(u.get("fueltech_id") or "")}),
            html.Td(f"{cap:,.1f}" if cap is not None else "—",
                    style={"fontSize": "11px", "padding": "2px 4px",
                           "textAlign": "right"}),
            html.Td(u.get("dispatch_type") or "—",
                    style={"fontSize": "10px", "padding": "2px 4px",
                           "color": "#666"}),
        ]))

    return [
        html.Div(
            f"Units ({len(unit_details)})",
            style={"marginTop": "14px", "fontSize": "12px",
                   "fontWeight": 600, "color": "#444"},
        ),
        html.Table(
            rows,
            style={"width": "100%", "borderCollapse": "collapse",
                   "marginTop": "4px",
                   "border": "1px solid #eee"},
        ),
    ]


def build_integration_section(facility_name: str | None,
                              network_region: str | None) -> list:
    """Render the Assignment-1 integration block in the popup.

    This is the visible answer to the report question "how do you
    integrate the MQTT messages with your existing data from
    Assignment 1?". Extension point: add more joined sources here
    (e.g. cer_accredited_geo) — single place to edit.
    """
    if not _DB_ENABLED:
        return []

    integ = task4_db.get_integration(facility_name, network_region)
    nger = integ.get("nger")
    abs_ctx = integ.get("abs")

    if not nger and not abs_ctx:
        return [
            html.Div(
                "No Assignment-1 reference match for this facility.",
                style={"marginTop": "14px", "fontSize": "11px",
                       "color": "#999", "fontStyle": "italic"},
            )
        ]

    children: list = [
        html.Div(
            "Integrated with Assignment 1",
            style={
                "marginTop": "16px", "marginBottom": "6px",
                "fontSize": "12px", "fontWeight": 700,
                "color": "#2c3e50", "borderTop": "2px solid #2c3e50",
                "paddingTop": "8px",
            },
        )
    ]

    def small_row(label: str, value: str) -> html.Div:
        return html.Div(
            [
                html.Span(label, style={"color": "#777", "fontSize": "11px"}),
                html.Span(value, style={"fontWeight": 600, "fontSize": "12px"}),
            ],
            style={"display": "flex", "justifyContent": "space-between",
                   "padding": "3px 0"},
        )

    if nger:
        children.append(
            html.Div(
                f"NGER history — matched “{nger['facility_name']}” "
                f"({integ.get('nger_years', 0)} financial years)",
                style={"fontSize": "11px", "color": "#555",
                       "marginBottom": "4px"},
            )
        )
        ei = nger.get("emission_intensity")
        children += [
            small_row(f"Emissions {nger['year_range']}",
                      f"{(nger['total_emissions_tco2e'] or 0):,.0f} tCO₂e"),
            small_row("Emission intensity",
                      f"{ei:.3f}" if ei is not None else "—"),
            small_row("Generation",
                      f"{(nger['electricity_production_mwh'] or 0):,.0f} MWh"),
        ]

    if abs_ctx:
        children.append(
            html.Div(
                f"State economy — {abs_ctx['state']} ({abs_ctx['year']}, ABS)",
                style={"fontSize": "11px", "color": "#555",
                       "marginTop": "8px", "marginBottom": "4px"},
            )
        )
        nb = abs_ctx.get("total_number_of_businesses")
        pe = abs_ctx.get("total_persons_employed")
        children.append(
            small_row("Businesses in state",
                      f"{nb:,.0f}" if nb is not None else "—")
        )
        children.append(
            small_row("Persons employed 15+",
                      f"{pe:,.0f}" if pe is not None else "n/a (ABS)")
        )

    return children


# ---------------------------------------------------------------------------
# 7. DASH LAYOUT
# ---------------------------------------------------------------------------

app = dash.Dash(__name__, title="COMP5339 — NEM Live")
app.layout = html.Div(
    [
        # ---- Header / KPI bar -------------------------------------------
        html.Div(
            [
                html.Div(
                    "COMP5339 — NEM Live Dashboard",
                    style={"fontSize": "20px", "fontWeight": 700},
                ),
                html.Div(id="stat-bar", style={"display": "flex", "gap": "28px"}),
            ],
            style={
                "display": "flex",
                "justifyContent": "space-between",
                "alignItems": "center",
                "padding": "12px 20px",
                "borderBottom": "1px solid #ddd",
                "backgroundColor": "#fafafa",
            },
        ),

        # ---- Filter bar (horizontal, replaces the old left column) ----
        html.Div(
            [
                # Metric toggle: horizontal radio
                html.Div(
                    [
                        html.Span("Show:", style={"fontWeight": 600,
                                                  "fontSize": "12px",
                                                  "color": "#555",
                                                  "marginRight": "8px"}),
                        dcc.RadioItems(
                            id="metric-toggle",
                            options=[
                                {"label": " Power",     "value": "power"},
                                {"label": " Emissions", "value": "emissions"},
                            ],
                            value="power",
                            inline=True,
                            labelStyle={"marginRight": "12px",
                                        "fontSize": "12px"},
                        ),
                    ],
                    style={"display": "flex", "alignItems": "center"},
                ),
                # Region multi-select dropdown
                html.Div(
                    [
                        html.Span("Region:", style={"fontWeight": 600,
                                                    "fontSize": "12px",
                                                    "color": "#555",
                                                    "marginRight": "6px"}),
                        dcc.Dropdown(
                            id="region-filter",
                            options=[{"label": r, "value": r}
                                     for r in NEM_REGIONS],
                            value=list(NEM_REGIONS),
                            multi=True,
                            clearable=False,
                            style={"width": "220px", "fontSize": "12px"},
                        ),
                    ],
                    style={"display": "flex", "alignItems": "center"},
                ),
                # Fueltech multi-select dropdown
                html.Div(
                    [
                        html.Span("Tech:", style={"fontWeight": 600,
                                                  "fontSize": "12px",
                                                  "color": "#555",
                                                  "marginRight": "6px"}),
                        dcc.Dropdown(
                            id="fueltech-filter",
                            options=[{"label": g, "value": g}
                                     for g in FUELTECH_GROUPS],
                            value=list(FUELTECH_GROUPS),
                            multi=True,
                            clearable=False,
                            style={"width": "320px", "fontSize": "12px"},
                        ),
                    ],
                    style={"display": "flex", "alignItems": "center"},
                ),
            ],
            style={
                "display": "flex",
                "gap": "20px",
                "padding": "10px 20px",
                "borderBottom": "1px solid #ddd",
                "backgroundColor": "#fafafa",
                "flexWrap": "wrap",
            },
        ),

        # ---- Market strip (optional Task 1: price + regional demand) ----
        # Five small tiles, one per NEM region, showing the most recently
        # received spot price and operational demand. Filled by the
        # `refresh_market_strip` callback on each poll tick.
        html.Div(
            id="market-strip",
            children=[],  # populated by callback
            style={
                "display": "flex",
                "gap": "10px",
                "padding": "8px 20px",
                "borderBottom": "1px solid #ddd",
                "backgroundColor": "#f4f7fa",
                "flexWrap": "wrap",
            },
        ),

        # ---- Main area: [collapsible list] | map | popup ----------------
        html.Div(
            [
                # Facility list (collapsible, Need 2)
                html.Div(
                    id="facility-list-panel",
                    children=[
                        html.Button(
                            "« Hide list",
                            id="toggle-facility-list",
                            n_clicks=0,
                            style={
                                "width": "100%",
                                "padding": "8px",
                                "fontSize": "11px",
                                "fontWeight": 600,
                                "border": "none",
                                "borderBottom": "1px solid #ddd",
                                "backgroundColor": "#f4f4f4",
                                "cursor": "pointer",
                                "textAlign": "center",
                                "color": "#444",
                            },
                        ),
                        html.Div(
                            id="facility-list-content",
                            children=[
                                html.Div(
                                    [
                                        html.Span("Facilities",
                                                  style={"fontWeight": 600,
                                                         "fontSize": "13px"}),
                                        html.Span(id="facility-list-count",
                                                  style={"marginLeft": "8px",
                                                         "color": "#888",
                                                         "fontSize": "11px"}),
                                    ],
                                    style={"padding": "10px 12px 6px 12px"},
                                ),
                                dash_table.DataTable(
                                    id="facility-list",
                                    columns=[
                                        {"name": "Name",   "id": "facility_name"},
                                        {"name": "Region", "id": "region"},
                                        {"name": "Tech",   "id": "fueltech_group"},
                                        {"name": "Cap MW", "id": "capacity_mw",
                                         "type": "numeric",
                                         "format": {"specifier": ",.0f"}},
                                        {"name": "Now MW", "id": "power_mw",
                                         "type": "numeric",
                                         "format": {"specifier": ",.1f"}},
                                    ],
                                    data=[],
                                    sort_action="native",
                                    sort_by=[{"column_id": "power_mw",
                                              "direction": "desc"}],
                                    row_selectable="single",
                                    selected_rows=[],
                                    page_action="none",
                                    style_table={"overflowY": "auto",
                                                 "height": "640px"},
                                    style_cell={"fontSize": "11px",
                                                "padding": "4px 6px",
                                                "fontFamily": "system-ui"},
                                    style_header={"fontSize": "10px",
                                                  "fontWeight": 700,
                                                  "backgroundColor": "#f4f4f4",
                                                  "textTransform": "uppercase",
                                                  "color": "#555"},
                                    style_data_conditional=[
                                        {"if": {"state": "selected"},
                                         "backgroundColor": "#e8f0fe",
                                         "border": "1px solid #2c3e50"},
                                    ],
                                ),
                            ],
                        ),
                    ],
                    style={"width": "290px",
                           "borderRight": "1px solid #ddd",
                           "backgroundColor": "#fdfdfd",
                           "transition": "width 0.2s"},
                ),

                # Map
                html.Div(
                    [dcc.Graph(id="facility-map", clear_on_unhover=False)],
                    style={"flex": "1", "minWidth": "0"},
                ),

                # Popup panel
                html.Div(
                    id="popup-panel",
                    children=build_popup_card(None),
                    style={
                        "width": "320px",
                        "padding": "16px",
                        "borderLeft": "1px solid #ddd",
                        "backgroundColor": "#fcfcfc",
                        "overflowY": "auto",
                        "maxHeight": "720px",
                    },
                ),
            ],
            style={"display": "flex", "height": "720px"},
        ),

        # Polling interval — the heart of the live-update mechanism.
        dcc.Interval(id="poll", interval=REFRESH_INTERVAL_MS, n_intervals=0),

        # Persists the currently-clicked facility code across renders.
        dcc.Store(id="selected-facility", data=None),
        # Persists the open/closed state of the facility list panel.
        dcc.Store(id="list-open", data=True),
    ],
    style={"fontFamily": "system-ui, -apple-system, sans-serif"},
)


# ---------------------------------------------------------------------------
# 8. CALLBACKS
# ---------------------------------------------------------------------------

@app.callback(
    Output("facility-map", "figure"),
    Output("stat-bar", "children"),
    Input("poll", "n_intervals"),
    Input("metric-toggle", "value"),
    Input("region-filter", "value"),
    Input("fueltech-filter", "value"),
    Input("selected-facility", "data"),
)
def refresh_map(n, metric, regions, fueltech_groups, selected_code):  # noqa: ARG001
    state, last_at = get_state_snapshot()
    df = build_dataframe(
        state,
        regions or [],
        fueltech_groups or [],
    )
    fig = build_map_figure(df, metric, selected_code=selected_code)

    # KPI cells in the header.
    def cell(label: str, value: str) -> html.Div:
        return html.Div(
            [
                html.Div(label, style={"fontSize": "11px", "color": "#888"}),
                html.Div(value, style={"fontSize": "16px", "fontWeight": 600}),
            ]
        )

    total_facilities = len(df)
    total_power = df["power_mw"].sum() if not df.empty else 0.0
    total_emissions = df["emissions_t"].sum() if not df.empty else 0.0
    last_str = last_at.strftime("%H:%M:%S") if last_at else "—"

    stats = [
        cell("Facilities", f"{total_facilities:,}"),
        cell("Σ Power",    f"{total_power:,.0f} MW"),
        cell("Σ Emissions", f"{total_emissions:,.1f} t"),
        cell("Last msg",   last_str),
    ]
    if _DB_ENABLED:
        db_stats = task4_db.stream_stats()
        stats.append(
            cell("DB rows", f"{db_stats.get('observations', 0):,}")
        )
    return fig, stats


# ---- Market strip (optional Task 1) -----------------------------------

def _build_market_tile(region: str, m: dict | None) -> html.Div:
    """One small region tile for the market strip.

    `m` is either the most recent market message for this region (from
    MARKET_STATE) or None when nothing has been received yet. In the
    latter case the tile still renders, with em-dashes for the values,
    so the user can see that the slot exists.
    """
    if m:
        price = m.get("price_aud_mwh")
        demand = m.get("demand_mw_region")
        price_s = f"${price:,.2f}/MWh" if price is not None else "—"
        demand_s = f"{demand:,.0f} MW" if demand is not None else "—"
        et = format_event_time(m.get("event_time"))
        body_colour = "#1f3a5f"
    else:
        price_s = "—"
        demand_s = "—"
        et = "waiting…"
        body_colour = "#999"
    return html.Div(
        [
            html.Div(
                region,
                style={"fontSize": "11px", "fontWeight": 700,
                       "color": "#555", "letterSpacing": "0.4px"},
            ),
            html.Div(
                price_s,
                style={"fontSize": "15px", "fontWeight": 700,
                       "color": body_colour},
            ),
            html.Div(
                [
                    html.Span("Demand: ", style={"color": "#888"}),
                    html.Span(demand_s, style={"fontWeight": 600,
                                               "color": body_colour}),
                ],
                style={"fontSize": "11px"},
            ),
            html.Div(
                et,
                style={"fontSize": "10px", "color": "#aaa",
                       "marginTop": "2px"},
            ),
        ],
        style={
            "flex": "1 1 0",
            "minWidth": "120px",
            "padding": "6px 10px",
            "border": "1px solid #d8dde3",
            "borderRadius": "6px",
            "backgroundColor": "#ffffff",
        },
    )


@app.callback(
    Output("market-strip", "children"),
    Input("poll", "n_intervals"),
)
def refresh_market_strip(n):  # noqa: ARG001
    """Repaint the five per-region market tiles on every poll tick.

    Reads the in-memory MARKET_STATE first (fast); if the dashboard has
    just restarted and that dict is empty, the DB is consulted once as
    a warm-up so the user is not staring at em-dashes after a restart
    when historical price/demand data IS already on disk.
    """
    snap = get_market_snapshot()
    if not snap and _DB_ENABLED:
        # Warm-up read: populate the in-memory state from the DB so the
        # tiles are not empty after a dashboard restart. Done at most
        # once per empty-state poll; the next MQTT message will keep
        # MARKET_STATE current from then on.
        snap = task4_db.get_market_latest_by_region()
        # Best-effort: copy back so subsequent reads are zero-cost.
        with _STATE_LOCK:
            for r, m in snap.items():
                _MARKET_STATE.setdefault(r, m)
    return [_build_market_tile(r, snap.get(r)) for r in NEM_REGIONS]


@app.callback(
    Output("facility-list", "data"),
    Output("facility-list-count", "children"),
    Input("poll", "n_intervals"),
    Input("region-filter", "value"),
    Input("fueltech-filter", "value"),
)
def refresh_facility_list(n, regions, fueltech_groups):  # noqa: ARG001
    """Populate the left-hand facility list (Need 2).

    Reuses the same `build_dataframe` filter pipeline as the map, so the
    list and the map are always in sync (filter region/fueltech once,
    both views update).
    """
    state, _ = get_state_snapshot()
    df = build_dataframe(state, regions or [], fueltech_groups or [])
    if df.empty:
        return [], "(0)"
    rows = (
        df[["facility_code", "facility_name", "region",
            "fueltech_group", "capacity_mw", "power_mw"]]
        .sort_values("power_mw", ascending=False)
        .to_dict("records")
    )
    return rows, f"({len(rows)})"


@app.callback(
    Output("selected-facility", "data"),
    Input("facility-map", "clickData"),
    Input("facility-list", "selected_rows"),
    State("facility-list", "data"),
    State("selected-facility", "data"),
)
def handle_select(click_data, selected_rows, list_data, current):
    """Update the selected-facility store from either source.

    A click on the map and a row selection in the list both feed into
    the same `selected-facility` Store, so downstream (popup) only
    has to listen to one input. Uses callback_context to determine
    which input actually fired this round.
    """
    trigger = dash.callback_context.triggered
    if not trigger:
        return current
    src = trigger[0]["prop_id"].split(".")[0]

    if src == "facility-map" and click_data:
        try:
            return click_data["points"][0]["customdata"][0]
        except (KeyError, IndexError, TypeError):
            return current

    if src == "facility-list" and selected_rows and list_data:
        try:
            return list_data[selected_rows[0]]["facility_code"]
        except (KeyError, IndexError, TypeError):
            return current

    return current


@app.callback(
    Output("popup-panel", "children"),
    Input("selected-facility", "data"),
    Input("poll", "n_intervals"),  # so the popup numbers also update live
)
def refresh_popup(facility_code, n):  # noqa: ARG001
    if not facility_code:
        return build_popup_card(None)
    state, _ = get_state_snapshot()
    return build_popup_card(state.get(facility_code))


# ---- Collapsible facility-list panel ----------------------------------

@app.callback(
    Output("list-open", "data"),
    Input("toggle-facility-list", "n_clicks"),
    State("list-open", "data"),
    prevent_initial_call=True,
)
def toggle_list_open(_n, is_open):
    """Flip the open/closed state on every button click."""
    return not bool(is_open)


@app.callback(
    Output("facility-list-panel", "style"),
    Output("facility-list-content", "style"),
    Output("toggle-facility-list", "children"),
    Input("list-open", "data"),
)
def apply_list_open(is_open):
    """Reflect the list-open state in the panel width and button label.

    When closed the panel shrinks to 32px, leaving just the toggle
    button visible on the left so it can be reopened. When open it
    expands back to 290px and shows the table.
    """
    base_panel_style = {
        "borderRight": "1px solid #ddd",
        "backgroundColor": "#fdfdfd",
        "transition": "width 0.2s",
        "overflow": "hidden",
    }
    if is_open:
        panel = {**base_panel_style, "width": "290px"}
        content = {"display": "block"}
        label = "« Hide list"
    else:
        panel = {**base_panel_style, "width": "32px"}
        content = {"display": "none"}
        label = "»"
    return panel, content, label


# ---------------------------------------------------------------------------
# 9. ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("[main] Starting MQTT subscriber…")
    client = start_subscriber()
    try:
        print("[main] Starting Dash server on http://127.0.0.1:8050")
        app.run(debug=False, host="127.0.0.1", port=8050)
    finally:
        print("[main] Stopping MQTT subscriber…")
        client.loop_stop()
        client.disconnect()
        if _DB_ENABLED:
            task4_db.close()
