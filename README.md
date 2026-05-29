# README

This project implements a real-time dashboard for displaying continuous power generation and emissions data.

The dashboard receives live MQTT messages, stores and reads data from a local DuckDB database, and visualises the latest facility-level and state-level information using Dash and Plotly.

## 1. Project Structure

The main files used in this project are:

```text
.
├── dashboard_prototype.py      # Main Dash dashboard application
├── db.py                       # Database helper functions
├── requirements.txt            # Python package requirements
├── energy.duckdb               # Local DuckDB database
├── data/                       # Input CSV or reference data files
└── README.md                   # Instructions for running the project
```

The exact file names may be slightly different depending on the local version of the project.

## 2. Requirements

This project requires Python 3.10 or above.

Install the required Python packages with:

```bash
pip install -r requirements.txt
```

The `requirements.txt` file includes:

```txt
dash>=2.17,<4.0
plotly>=5.24
paho-mqtt>=2.0,<3.0
pandas>=2.0
duckdb>=1.0
```

## 3. Data Preparation

The main reference tables must be included in the `data/` folder:

```text
nger_emissions_enriched
cer_accredited_geo
abs_economy
```

If the database file does not exist, run the database initialisation code first, or make sure the required CSV files are placed in the correct `data/` folder.

The project uses `energy.duckdb` as the local database for storing reference data and real-time MQTT observation data.

## 4. MQTT Setup

Before starting the dashboard, make sure the MQTT broker is running.

If the MQTT broker address, port, or topic is different from the default settings, update the corresponding values in the Python code before running the dashboard.

## 5. Running the Dashboard

To start the dashboard, run:

```bash
python dashboard_prototype.py
```

After the program starts successfully, open the dashboard in a web browser:

```text
http://127.0.0.1:8050
```

## 6. Running the Publisher

The MQTT publisher was written in Jupyter Notebook.

To start publishing live data, open the publisher notebook and run the cells from Step 1 to Step 3 in order.

The publisher should be started after the MQTT broker is running. It sends live power generation and emissions messages to the MQTT topic used by the dashboard subscriber.

## 7. Expected Running Process

The expected running process is:

1. Install the required packages using `requirements.txt`.
2. Make sure `energy.duckdb` and the required data files are available.
3. Start the MQTT broker or make sure the broker is already running.
4. Run the Dash application.
5. Open the dashboard in a web browser.
6. Start the MQTT publisher if live data needs to be streamed into the dashboard.

## 8. Notes

* The dashboard application runs on the main thread.
* MQTT subscribers run in background threads.
* Incoming MQTT messages are stored in DuckDB.
* Dash callbacks periodically refresh the visualisations and tables.
* If no new MQTT messages are received, the dashboard may only show existing records from the database.
* If the dashboard page opens but no data is displayed, check whether the MQTT broker, publisher, and database are working correctly.

## 9. Stopping the Program

To stop the dashboard, press:

```bash
Ctrl + C
```

in the terminal where the program is running.
