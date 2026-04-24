# Airport Operations Analytics Pipeline

**MSBA 305 – Data Processing Framework | Spring 2025/2026**

An end-to-end data processing pipeline that integrates historical flight operations data, airport reference metadata, and real-time flight status into a single query-ready analytical dataset. Designed to surface the upstream drivers of airport congestion and delay propagation.

---

## Domain Summary

Airport operations face pressure from unpredictable arrival volumes, clustered flight arrivals, and propagated delays that collectively degrade downstream processing (passport control, baggage, security). This pipeline diagnoses those upstream drivers so operational teams can benchmark performance across airports, decompose delay contributors, and detect month-over-month trend shifts.

---

## Architecture Overview

```
  Data Sources                Ingestion              Cleaning / Integration           Analytics
  ------------                ---------              ----------------------           ---------
  BTS CSV ------------>    pandas read_csv  ------>  dropna + type cast        ---\
                                                                                  >---> merged_df ---> 5 analytical queries
  OpenFlights URL ---->    pandas read_csv  ------>  IATA filter + rename      ---/                    top-N airports
                                                                                                       country volume
  AviationStack API -->    requests.get     ------>  JSON flatten + dropna     ---> real-time enrich   delay decomposition
                           with retry/timeout                                                          rolling windows
```

Full architecture diagram: see `diagrams/aviation_pipeline_diagram.png` in this repository.

---

## Repository Layout

```
msba305/
├── code/
│   └── pipeline.py              # full end-to-end pipeline (ingest -> query)
├── notebooks/
│   └── pipeline_demo.ipynb      # narrated walkthrough with rendered outputs
├── data/
│   ├── bts_sample.csv           # 200-row structural sample for reproducibility
│   └── (drop Airline_Delay_Cause.csv here for a full run; gitignored)
├── diagrams/
│   ├── aviation_pipeline_diagram.png   # pipeline architecture (Appendix A)
│   ├── er_schema_diagram.png           # ER / schema diagram (§A4)
│   ├── roadmap_timeline.png            # scalability roadmap (§A12.2)
│   ├── risk_heatmap.png                # risk heatmap (§A12.4)
│   ├── dashboard_mockup.png            # Phase 2 dashboard mockup (§A8)
│   └── chart_1..6_*.png                # analytical charts (§A8)
├── report/
│   ├── MSBA305_Final_Report.docx       # main deliverable
│   └── CHECKLIST_CROSSCHECK.docx       # rubric cross-check matrix
├── README.md
├── requirements.txt
└── .env.example                        # template for required secrets
```

---

## Setup

### 1. Clone and create a virtual environment

```bash
git clone <repo-url>
cd msba305
python -m venv venv
source venv/bin/activate        # on Windows: venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Get the data files

- **BTS Airline Delay Cause** — download `Airline_Delay_Cause.csv` from the [Kaggle dataset by sriharshaeedala](https://www.kaggle.com/datasets/sriharshaeedala/airline-delay) and place it in `data/`.
- **OpenFlights** — fetched automatically at runtime from its public GitHub mirror.
- **AviationStack** — create a free account at [aviationstack.com](https://aviationstack.com) and copy your access key.

### 4. Configure environment variables

Copy `.env.example` to `.env` and fill in your values:

```
FLIGHT_DATA_PATH=data/Airline_Delay_Cause.csv
AVIATIONSTACK_API_KEY=your_key_here
```

These are read by `os.getenv` inside `pipeline.py`. The API key is **never** hardcoded in source.

### 5. Run the pipeline

```bash
python code/pipeline.py
```

You should see timestamped log lines for each stage, five analytical query outputs, and four matplotlib visualizations.

### 6. Explore the notebook (optional)

```bash
pip install jupyter
jupyter notebook notebooks/pipeline_demo.ipynb
```

The notebook walks through ingestion, cleaning, integration, queries Q1–Q5, and visualisations with narrative and rendered outputs. A 200-row sample (`data/bts_sample.csv`) lets the notebook run end-to-end without the full BTS download.

---

## Analytical Queries

The pipeline produces five queries of increasing complexity:

| # | Type | Question |
|---|------|----------|
| Q1 | Filter | Which airport-months had >10,000 minutes of arrival delay? |
| Q2 | Aggregation | Total flights per country |
| Q3 | Join + group-by | Top 10 airports by volume, enriched with city/country metadata |
| Q4 | Window function | 3-month rolling average delay for a selected airport |
| Q5 | Multi-dim pivot | Delay-type decomposition (weather / NAS / late-aircraft %) per top airport |

---

## Error Handling & Robustness

- CSV ingestion wrapped in `try/except` for `FileNotFoundError` and `pd.errors.ParserError`.
- AviationStack API calls use exponential backoff with configurable retries (`MAX_API_RETRIES=3`).
- HTTP timeout set at 30 seconds to prevent hangs.
- Schema validation on ingestion: missing required columns fails loudly before downstream stages.
- Graceful degradation: if the API is unavailable, the historical analysis path still completes.

---

## Known Limitations

- Geographic skew: BTS covers only U.S.-certified carriers, so country-level aggregation over-represents the U.S.
- AviationStack free tier returns ~100 records per request with a ~90–95% null rate on the `delay` field, limiting its analytical weight.
- In-memory pandas processing is appropriate at current scale; a move to PostgreSQL or Spark would be required for >10× data growth (see report §A4).

---

## Author

Mohamad Hassan | MSBA Program | American University of Beirut
