"""
Airport Operations Analytics Pipeline
MSBA 305 - Data Processing Framework (Spring 2025/2026)

End-to-end pipeline: ingestion -> cleaning -> integration -> analytical queries -> visualization.
Integrates three sources: BTS flight delays (CSV), OpenFlights airport metadata (URL), and
AviationStack real-time flight status (REST API / JSON).

Run:
    python pipeline.py

Environment variables required:
    FLIGHT_DATA_PATH       - local path to Airline_Delay_Cause.csv (BTS)
    AVIATIONSTACK_API_KEY  - personal access key for aviationstack.com
Both can be set in a .env file (see README.md).
"""

import os
import sys
import logging
import time
from typing import Optional

import pandas as pd
import requests
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Load from environment; never hardcode credentials in source.
FLIGHT_DATA_PATH = os.getenv("FLIGHT_DATA_PATH", "data/Airline_Delay_Cause.csv")
OPENFLIGHTS_URL = (
    "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
)
AVIATIONSTACK_API_KEY = os.getenv("AVIATIONSTACK_API_KEY", "")
AVIATIONSTACK_URL = "http://api.aviationstack.com/v1/flights"

REQUEST_TIMEOUT_SECONDS = 30
MAX_API_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

# Logging configuration: timestamped, level-tagged, writes to stdout.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("airport_pipeline")


# ---------------------------------------------------------------------------
# Ingestion layer
# ---------------------------------------------------------------------------

def ingest_flight_delays(path: str) -> pd.DataFrame:
    """
    Load the BTS Airline Delay Cause dataset from a local CSV.

    Wraps file I/O in try/except so a missing or malformed CSV fails
    loudly with context rather than silently propagating NaNs.
    """
    log.info("Ingesting BTS flight delay dataset from %s", path)
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        log.error("Flight delay CSV not found at %s. Set FLIGHT_DATA_PATH.", path)
        raise
    except pd.errors.ParserError as exc:
        log.error("Flight delay CSV is malformed: %s", exc)
        raise

    required_cols = {
        "year", "month", "airport", "airport_name",
        "arr_flights", "arr_delay",
        "weather_delay", "nas_delay", "late_aircraft_delay",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Flight delay CSV missing expected columns: {missing}")

    df = df[list(required_cols)]
    log.info("Loaded %d flight-delay rows", len(df))
    return df


def ingest_airports(url: str) -> pd.DataFrame:
    """
    Load the OpenFlights airport reference dataset directly from URL.

    The source file has no header; column names are assigned positionally.
    """
    log.info("Ingesting OpenFlights airport metadata from %s", url)
    cols = [
        "airport_id", "name", "city", "country", "iata", "icao",
        "lat", "lon", "altitude", "timezone", "dst", "tz", "type", "source",
    ]
    try:
        airports = pd.read_csv(url, names=cols)
    except Exception as exc:  # network or parse failure
        log.error("Failed to fetch OpenFlights data: %s", exc)
        raise

    airports = airports[["iata", "name", "city", "country"]]
    airports = airports[airports["iata"] != "\\N"]
    log.info("Loaded %d airport metadata rows (post-filter)", len(airports))
    return airports


def ingest_realtime_flights(api_key: str) -> Optional[pd.DataFrame]:
    """
    Fetch a real-time flight snapshot from AviationStack with retry/backoff.

    Returns None (rather than raising) on repeated API failure so the
    historical analysis path is not blocked by an upstream outage.
    """
    if not api_key:
        log.warning("AVIATIONSTACK_API_KEY not set; skipping real-time ingestion.")
        return None

    log.info("Requesting real-time flight data from AviationStack")
    for attempt in range(1, MAX_API_RETRIES + 1):
        try:
            response = requests.get(
                AVIATIONSTACK_URL,
                params={"access_key": api_key},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
            break
        except (requests.RequestException, ValueError) as exc:
            log.warning("API attempt %d/%d failed: %s",
                        attempt, MAX_API_RETRIES, exc)
            if attempt == MAX_API_RETRIES:
                log.error("AviationStack unavailable after %d attempts; "
                          "continuing without real-time data.", MAX_API_RETRIES)
                return None
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    flights = payload.get("data", [])
    if not flights:
        log.warning("API returned empty 'data' array")
        return pd.DataFrame()

    rows = []
    for f in flights:
        try:
            rows.append({
                "flight_date": f.get("flight_date"),
                "departure_airport": (f.get("departure") or {}).get("iata"),
                "arrival_airport": (f.get("arrival") or {}).get("iata"),
                "delay": (f.get("arrival") or {}).get("delay"),
            })
        except (KeyError, TypeError) as exc:
            log.debug("Skipping malformed API record: %s", exc)

    api_df = pd.DataFrame(rows)
    log.info("Received %d real-time flight records", len(api_df))
    return api_df


# ---------------------------------------------------------------------------
# Cleaning and integration layer
# ---------------------------------------------------------------------------

def clean_flight_delays(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with nulls in core metrics; build a composite date field."""
    before = len(df)
    df = df.dropna()
    df = df.copy()
    df["date"] = df["year"].astype(str) + "-" + df["month"].astype(str)
    log.info("Flight delays cleaned: %d -> %d rows", before, len(df))
    return df


def integrate_sources(
    flights: pd.DataFrame,
    airports: pd.DataFrame,
    api_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """
    Merge flight delays with airport metadata on IATA code, then left-join
    any available real-time records for enrichment.
    """
    merged = flights.merge(
        airports, left_on="airport", right_on="iata", how="left"
    )
    merged = merged.dropna(subset=["city", "country"]).drop(columns=["name"])
    log.info("Post-integration dataset: %d rows, %d columns",
             len(merged), merged.shape[1])

    if api_df is not None and not api_df.empty:
        api_df = api_df.dropna(subset=["delay"])
        api_df = api_df[api_df["arrival_airport"].isin(merged["airport"])]
        final = merged.merge(
            api_df, left_on="airport", right_on="arrival_airport", how="left"
        )
        log.info("Real-time enrichment joined: %d matched records",
                 api_df.shape[0])
        return final

    log.info("No real-time data to join; returning historical-only dataset.")
    return merged


# ---------------------------------------------------------------------------
# Analytical queries (A7 deliverable - five queries, increasing complexity)
# ---------------------------------------------------------------------------

def query_1_filter_high_delay_months(df: pd.DataFrame) -> pd.DataFrame:
    """
    Q1 (Filter): Airport-months with total arrival delay above 10,000 minutes.
    Business question: Which airport-month combinations represent severe
    congestion episodes worth investigating?
    """
    result = df[df["arr_delay"] > 10_000][
        ["airport_name", "date", "arr_flights", "arr_delay"]
    ].sort_values("arr_delay", ascending=False).head(15)
    return result


def query_2_aggregation_country_volume(df: pd.DataFrame) -> pd.Series:
    """
    Q2 (Aggregation): Total flights by country.
    Business question: Where is air traffic concentrated?
    """
    return df.groupby("country")["arr_flights"].sum().sort_values(ascending=False)


def query_3_join_top_airports_enriched(df: pd.DataFrame) -> pd.DataFrame:
    """
    Q3 (Join + Aggregation): Top 10 airports by flight volume, enriched
    with city/country metadata from the OpenFlights join upstream.
    Business question: Which airports are the highest-volume hubs, and
    where are they located?
    """
    return (
        df.groupby(["airport", "airport_name", "city", "country"])
        .agg(total_flights=("arr_flights", "sum"),
             total_delay=("arr_delay", "sum"))
        .assign(mean_delay_per_flight=lambda x: x["total_delay"] / x["total_flights"])
        .sort_values("total_flights", ascending=False)
        .head(10)
        .reset_index()
    )


def query_4_window_rolling_delay(df: pd.DataFrame, airport_code: str = "ATL") -> pd.DataFrame:
    """
    Q4 (Window function): 3-month rolling average of arrival delay for a
    single airport.
    Business question: Is the airport's delay performance trending up or
    down over time?
    """
    ts = (
        df[df["airport"] == airport_code]
        .groupby("date")["arr_delay"].sum()
        .sort_index()
        .reset_index()
    )
    ts["rolling_3mo_avg"] = ts["arr_delay"].rolling(window=3, min_periods=1).mean()
    return ts


def query_5_pivot_delay_decomposition(df: pd.DataFrame) -> pd.DataFrame:
    """
    Q5 (Multi-dimensional pivot): Delay-type decomposition per top airport,
    shown as percentage shares of weather / NAS / late-aircraft delay.
    Business question: For each major hub, which delay category is the
    dominant contributor? Determines whether interventions should target
    scheduling (late aircraft), infrastructure (NAS), or external factors.
    """
    top_airports = query_3_join_top_airports_enriched(df)["airport"].tolist()
    subset = df[df["airport"].isin(top_airports)]

    decomposition = subset.groupby("airport_name").agg(
        weather=("weather_delay", "sum"),
        nas=("nas_delay", "sum"),
        late_aircraft=("late_aircraft_delay", "sum"),
    )
    decomposition["total"] = decomposition.sum(axis=1)
    for col in ["weather", "nas", "late_aircraft"]:
        decomposition[f"{col}_pct"] = (
            decomposition[col] / decomposition["total"] * 100
        ).round(1)

    return decomposition[["weather_pct", "nas_pct", "late_aircraft_pct", "total"]]


# ---------------------------------------------------------------------------
# Visualization layer
# ---------------------------------------------------------------------------

def plot_top_countries(country_totals: pd.Series, exclude_us: bool = False) -> None:
    data = country_totals.drop("United States") if exclude_us else country_totals
    title_suffix = " (excluding USA)" if exclude_us else ""
    data.head(10).plot(kind="bar")
    plt.title(f"Top Countries by Flight Volume{title_suffix}")
    plt.ylabel("Number of Flights")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()


def plot_top_airports(airport_ranking: pd.DataFrame) -> None:
    airport_ranking.set_index("airport_name")["total_flights"].plot(kind="bar")
    plt.title("Top 10 Airports by Flight Volume")
    plt.ylabel("Number of Flights")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()


def plot_delay_types(df: pd.DataFrame) -> None:
    totals = df[["weather_delay", "nas_delay", "late_aircraft_delay"]].sum()
    totals.plot(kind="bar")
    plt.title("Total Delays by Type")
    plt.ylabel("Total Delay (minutes)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    """End-to-end orchestration; each stage fails loudly with context."""
    try:
        flights_raw = ingest_flight_delays(FLIGHT_DATA_PATH)
        airports = ingest_airports(OPENFLIGHTS_URL)
        api_df = ingest_realtime_flights(AVIATIONSTACK_API_KEY)
    except Exception as exc:
        log.critical("Ingestion failed: %s", exc)
        sys.exit(1)

    flights = clean_flight_delays(flights_raw)
    merged = integrate_sources(flights, airports, api_df)

    log.info("=== Q1: High-delay airport-months ===")
    print(query_1_filter_high_delay_months(merged))

    log.info("=== Q2: Flights by country (top 5) ===")
    print(query_2_aggregation_country_volume(merged).head())

    log.info("=== Q3: Top 10 airports enriched ===")
    print(query_3_join_top_airports_enriched(merged))

    log.info("=== Q4: Rolling 3-month delay for ATL ===")
    print(query_4_window_rolling_delay(merged, airport_code="ATL"))

    log.info("=== Q5: Delay-type decomposition ===")
    print(query_5_pivot_delay_decomposition(merged))

    plot_top_countries(query_2_aggregation_country_volume(merged))
    plot_top_countries(query_2_aggregation_country_volume(merged), exclude_us=True)
    plot_top_airports(query_3_join_top_airports_enriched(merged))
    plot_delay_types(merged)


if __name__ == "__main__":
    run_pipeline()
