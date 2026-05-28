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
import plotly.graph_objects as go
from dash import Input, Output, State, dcc, html

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

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
MQTT_TOPIC = "comp5339/electricity/facility/#"  # wildcard — see PROJECT_DOC §4
CLIENT_ID = "comp5339_dashboard_subscriber"

# How often the dashboard polls the shared state and re-renders. The
# prototype is intentionally conservative; if the publisher is producing
# 10+ messages/sec, drop this to 1000 ms.
REFRESH_INTERVAL_MS = 2000

# Map starting viewport — centred roughly on south-east Australia (NEM area).
MAP_CENTER = {"lat": -32.5, "lon": 145.0}
MAP_ZOOM = 4.2

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
        print(f"[MQTT] Connected. Subscribing to {MQTT_TOPIC!r}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"[MQTT] Connect failed: {reason_code}")


def _on_message(client, userdata, msg):  # noqa: ARG001
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        print(f"[MQTT] Bad payload on {msg.topic}: {e}")
        return
    update_state(payload)

    # Task 4: persist to energy.duckdb in parallel with the in-memory
    # state. Wrapped so a DB failure can never kill the subscriber loop.
    if _DB_ENABLED:
        task4_db.persist_observation(payload)


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


def build_map_figure(df: pd.DataFrame, metric: str) -> go.Figure:
    """Build the scatter_map figure. `metric` ∈ {'power', 'emissions'}."""
    fig = go.Figure()

    if df.empty:
        fig.update_layout(
            map={"style": "open-street-map", "center": MAP_CENTER, "zoom": MAP_ZOOM},
            margin={"l": 0, "r": 0, "t": 0, "b": 0},
            height=720,
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
        )
    )

    fig.update_layout(
        map={"style": "open-street-map", "center": MAP_CENTER, "zoom": MAP_ZOOM},
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        height=720,
        showlegend=False,
        uirevision="keep",  # preserves user's pan/zoom across refreshes
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

        # ---- Main area: controls | map | popup --------------------------
        html.Div(
            [
                # Controls
                html.Div(
                    [
                        html.Div("Display metric", style={"fontWeight": 600, "marginBottom": "6px"}),
                        dcc.RadioItems(
                            id="metric-toggle",
                            options=[
                                {"label": " Power",     "value": "power"},
                                {"label": " Emissions", "value": "emissions"},
                            ],
                            value="power",
                            labelStyle={"display": "block", "marginBottom": "4px"},
                        ),

                        html.Hr(),
                        html.Div("Region", style={"fontWeight": 600, "marginBottom": "6px"}),
                        dcc.Checklist(
                            id="region-filter",
                            options=[{"label": f" {r}", "value": r} for r in NEM_REGIONS],
                            value=list(NEM_REGIONS),
                            labelStyle={"display": "block", "marginBottom": "4px"},
                        ),

                        html.Hr(),
                        html.Div("Fuel technology", style={"fontWeight": 600, "marginBottom": "6px"}),
                        dcc.Checklist(
                            id="fueltech-filter",
                            options=[{"label": f" {g}", "value": g} for g in FUELTECH_GROUPS],
                            value=list(FUELTECH_GROUPS),
                            labelStyle={"display": "block", "marginBottom": "4px"},
                        ),
                    ],
                    style={
                        "width": "200px",
                        "padding": "16px",
                        "borderRight": "1px solid #ddd",
                        "fontSize": "13px",
                    },
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
)
def refresh_map(n, metric, regions, fueltech_groups):  # noqa: ARG001
    state, last_at = get_state_snapshot()
    df = build_dataframe(
        state,
        regions or [],
        fueltech_groups or [],
    )
    fig = build_map_figure(df, metric)

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


@app.callback(
    Output("selected-facility", "data"),
    Input("facility-map", "clickData"),
    State("selected-facility", "data"),
)
def handle_click(click_data, current):
    if not click_data:
        return current
    try:
        return click_data["points"][0]["customdata"][0]
    except (KeyError, IndexError, TypeError):
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
