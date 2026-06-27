"""
Sea Ice Lineage Demo
====================
Mały, działający pipeline ETL z emisją zdarzeń OpenLineage.

Łańcuch (to zobaczysz jako graf w Marquezie):

    raw_sea_ice.csv ──[ingest]──► staging.sea_ice_raw
                                        │
                                  [clean]│
                                        ▼
                                 staging.sea_ice_clean
                                        │
                              [aggregate]│
                                        ▼
                                 marts.sea_ice_monthly

Uruchomienie:
    # 1) bez Marqueza – tylko sprawdzenie, że działa (eventy lecą na konsolę):
    OPENLINEAGE_TRANSPORT=console python pipeline.py

    # 2) z Marquezem (domyślnie http://localhost:5000):
    python pipeline.py
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    RunEvent, RunState, Run, Job, InputDataset, OutputDataset,
)
from openlineage.client.facet_v2 import schema_dataset
from openlineage.client.transport.console import ConsoleTransport, ConsoleConfig
from openlineage.client.transport.http import HttpTransport, HttpConfig
from openlineage.client.uuid import generate_new_uuid

# --- Konfiguracja ---------------------------------------------------------

# PRODUCER = identyfikator narzędzia, które wyemitowało event (Twoje repo).
PRODUCER = "https://github.com/Kacpeee/sea-ice-lineage"
# NAMESPACE = logiczna "przestrzeń nazw" danych (np. nazwa projektu/środowiska).
NAMESPACE = "sea_ice"
DATA_DIR = Path("data")


def build_client() -> OpenLineageClient:
    """Zwraca klienta OpenLineage. Domyślnie wysyła do Marqueza po HTTP,
    a przy OPENLINEAGE_TRANSPORT=console wypisuje eventy na ekran (do testów)."""
    if os.getenv("OPENLINEAGE_TRANSPORT") == "console":
        return OpenLineageClient(transport=ConsoleTransport(ConsoleConfig()))
    url = os.getenv("OPENLINEAGE_URL", "http://localhost:5000")
    return OpenLineageClient(transport=HttpTransport(HttpConfig(url=url)))


CLIENT = build_client()


# --- Pomocnicze: budowa datasetów i emisja eventów ------------------------

def schema_facet(*columns: tuple[str, str]) -> dict:
    """Tworzy facet 'schema' (lista kolumn: nazwa + typ) doczepiany do datasetu."""
    fields = [
        schema_dataset.SchemaDatasetFacetFields(name=name, type=col_type)
        for name, col_type in columns
    ]
    return {"schema": schema_dataset.SchemaDatasetFacet(fields=fields)}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit(event_type: RunState, run_id: str, job_name: str,
         inputs=None, outputs=None) -> None:
    """Wysyła pojedynczy RunEvent (START / COMPLETE / FAIL) do OpenLineage."""
    CLIENT.emit(
        RunEvent(
            eventTime=_now(),
            eventType=event_type,
            producer=PRODUCER,
            run=Run(runId=run_id),
            job=Job(namespace=NAMESPACE, name=job_name),
            inputs=inputs or [],
            outputs=outputs or [],
        )
    )


def run_job(job_name: str, inputs, outputs, work):
    """Opakowanie jednego kroku pipeline'u: emituje START, wykonuje pracę,
    a potem COMPLETE (albo FAIL, jeśli coś poleci wyjątkiem)."""
    run_id = str(generate_new_uuid())
    emit(RunState.START, run_id, job_name, inputs, outputs)
    try:
        result = work()
    except Exception:
        emit(RunState.FAIL, run_id, job_name, inputs, outputs)
        raise
    emit(RunState.COMPLETE, run_id, job_name, inputs, outputs)
    print(f"  [{job_name}] OK")
    return result


# --- Definicje datasetów (wejścia/wyjścia kolejnych kroków) ----------------

RAW = InputDataset(
    namespace=NAMESPACE, name="raw_sea_ice.csv",
    facets=schema_facet(("date", "STRING"), ("extent_raw", "STRING")),
)
STG_RAW_OUT = OutputDataset(
    namespace=NAMESPACE, name="staging.sea_ice_raw",
    facets=schema_facet(("date", "DATE"), ("extent", "DOUBLE")),
)
STG_RAW_IN = InputDataset(namespace=NAMESPACE, name="staging.sea_ice_raw",
                          facets=STG_RAW_OUT.facets)
STG_CLEAN_OUT = OutputDataset(
    namespace=NAMESPACE, name="staging.sea_ice_clean",
    facets=schema_facet(("date", "DATE"), ("extent", "DOUBLE"),
                        ("year", "INTEGER"), ("month", "INTEGER")),
)
STG_CLEAN_IN = InputDataset(namespace=NAMESPACE, name="staging.sea_ice_clean",
                            facets=STG_CLEAN_OUT.facets)
MART_OUT = OutputDataset(
    namespace=NAMESPACE, name="marts.sea_ice_monthly",
    facets=schema_facet(("year", "INTEGER"), ("month", "INTEGER"),
                        ("mean_extent", "DOUBLE"), ("n_obs", "INTEGER")),
)


# --- Logika ETL (zwykły pandas) -------------------------------------------

def make_raw_csv() -> Path:
    """Generuje syntetyczny surowy plik (celowo 'brudny': stringi, braki)."""
    DATA_DIR.mkdir(exist_ok=True)
    rng = np.random.default_rng(42)
    dates = pd.date_range("2015-01-01", "2024-12-31", freq="D")
    # sezonowy sygnał + szum (ekstent lodu morskiego, mln km^2)
    season = 12 + 6 * np.sin(2 * np.pi * (dates.dayofyear / 365.25))
    extent = season + rng.normal(0, 0.6, len(dates))
    df = pd.DataFrame({"date": dates.strftime("%Y-%m-%d"),
                       "extent_raw": np.round(extent, 3).astype(str)})
    # wstrzykujemy trochę "brudu": puste i ujemne wartości
    dirty = rng.choice(len(df), size=200, replace=False)
    df.loc[dirty[:100], "extent_raw"] = ""
    df.loc[dirty[100:], "extent_raw"] = "-1"
    path = DATA_DIR / "raw_sea_ice.csv"
    df.to_csv(path, index=False)
    return path

def ingest():
    path = make_raw_csv()
    df = pd.read_csv(path, dtype=str)
    df.to_parquet(DATA_DIR / "sea_ice_raw.parquet", index=False)
    return df

def clean():
    df = pd.read_parquet(DATA_DIR / "sea_ice_raw.parquet")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["extent"] = pd.to_numeric(df["extent_raw"], errors="coerce")
    df = df.dropna(subset=["date", "extent"])
    df = df[df["extent"] > 0]                     # usuwamy nierealne wartości
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df = df[["date", "extent", "year", "month"]]
    df.to_parquet(DATA_DIR / "sea_ice_clean.parquet", index=False)
    return df

def aggregate():
    df = pd.read_parquet(DATA_DIR / "sea_ice_clean.parquet")
    out = (df.groupby(["year", "month"])
             .agg(mean_extent=("extent", "mean"), n_obs=("extent", "size"))
             .reset_index())
    out["mean_extent"] = out["mean_extent"].round(3)
    out.to_csv(DATA_DIR / "sea_ice_monthly.csv", index=False)
    return out


# --- Orkiestracja ----------------------------------------------------------

def main():
    print("Uruchamiam pipeline z emisją lineage do OpenLineage...")
    run_job("ingest_sea_ice", inputs=[RAW], outputs=[STG_RAW_OUT], work=ingest)
    run_job("clean_sea_ice", inputs=[STG_RAW_IN], outputs=[STG_CLEAN_OUT], work=clean)
    result = run_job("aggregate_sea_ice", inputs=[STG_CLEAN_IN], outputs=[MART_OUT],
                     work=aggregate)
    print(f"\nGotowe. Wynik: {len(result)} wierszy miesięcznych w data/sea_ice_monthly.csv")
    print("Wejdz na http://localhost:3000 (Marquez), namespace 'sea_ice', zeby zobaczyc graf.")


if __name__ == "__main__":
    main()
