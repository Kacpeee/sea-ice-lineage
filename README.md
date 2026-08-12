# Sea Ice Data Lineage — OpenLineage + Marquez

A working demonstration of **end-to-end data lineage** on an ETL pipeline: the data
runs through a chain of transformations, and every step reports its own provenance
automatically — sources, outputs, schemas. What comes out is a clickable graph of the
whole path the data took, from a raw file to the aggregated output table.

The project sits where three areas meet: data engineering (the ETL pipeline), metadata
(lineage and schemas) and observability (tracking runs and how they ended).

![Lineage graph in Marquez](https://github.com/user-attachments/assets/5b5b6555-f975-42b1-8be3-5475e3a35a48)

*The lineage graph in Marquez: three steps (`ingest → clean → aggregate`) joined by the
datasets that pass between them, with each table's schema and how it changes along the way.*

---

## What this project demonstrates

- **Instrumenting a pipeline with OpenLineage** — `START` / `COMPLETE` / `FAIL` events
  emitted by hand from the code, on a correct Job / Run / Dataset model.
- **Metadata modeling** — the flow described as input→output relations, with column
  schemas (the `schema` facet) and their evolution from one stage to the next.
- **ETL with data quality in mind** — missing values and outliers dealt with, types
  enforced, aggregation (pandas).
- **Running infrastructure in Docker** — the lineage backend brought up locally
  (Marquez: API + PostgreSQL + UI + OpenSearch) and diagnosed from its logs.

---

## Architecture

```
  pipeline.py ──(OpenLineage events / HTTP)──►  Marquez API ──►  PostgreSQL
       │                                              │
       │ writes the data files                  Marquez Web UI  (lineage graph)
       ▼
   data/*.csv, *.parquet
```

The pipeline sends metadata *about* the flow to the Marquez API; the data itself
(CSV/parquet) goes to disk. Marquez collects the events and draws them as a graph.

**The pipeline, in three steps:**

```
raw_sea_ice.csv ──[ingest]──► staging.sea_ice_raw ──[clean]──► staging.sea_ice_clean ──[aggregate]──► marts.sea_ice_monthly
```

---

## The lineage model (OpenLineage)

| Concept | What it is | Here |
|---|---|---|
| **Job** | a processing step | `ingest_sea_ice`, `clean_sea_ice`, `aggregate_sea_ice` |
| **Run** | one execution of a job | one run of the pipeline (a separate `runId` per step) |
| **Dataset** | data going in or out | `raw_sea_ice.csv`, `staging.*`, `marts.*` |
| **Facet** | metadata attached to a dataset | `schema` — columns and their types |

Every step reports which datasets it read and which it produced. Marquez rebuilds the
graph from those relations alone.

---

## Stack

**Python** (pandas, NumPy) · **OpenLineage** (`openlineage-python`) ·
**Marquez** · **PostgreSQL** · **Docker / Docker Compose**

---

## Layout

| File | Contents |
|---|---|
| `pipeline.py` | the ETL pipeline and the OpenLineage event emission |
| `requirements.txt` | dependencies, pinned |
| `NOTATKI_konfiguracja.md` | setup notes and problems solved along the way |
| `WYJASNIENIE_kodu.md` | a walkthrough of the code, section by section |

---

## Running it

```bash
# 1. The lineage backend (Marquez in Docker)
git clone https://github.com/MarquezProject/marquez.git
cd marquez && ./docker/up.sh          # UI: localhost:3000 · API: localhost:5000

# 2. The pipeline
pip install -r requirements.txt
python pipeline.py                    # emits lineage to Marquez
```

To see it work without Marquez running, print the events to the console instead:
`OPENLINEAGE_TRANSPORT=console python pipeline.py`.
The graph itself lives at `localhost:3000`, under the `sea_ice` namespace.

---

## Where this could go next

- Collecting lineage automatically through **Airflow** and its OpenLineage provider,
  instead of emitting it by hand.
- A **dataQuality** facet — the number of rows dropped at the cleaning step, for instance.
- Real data (NSIDC Sea Ice Index) in place of the synthetic set.
- Loading the metadata into **OpenMetadata** as a data catalog.
