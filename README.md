# Sea Ice Data Lineage — demo OpenLineage + Marquez

Mały, działający projekt pokazujący **data lineage** (rodowód danych) na prostym
pipeline ETL. Pipeline przetwarza dane o zasięgu lodu morskiego, a przy okazji
emituje zdarzenia **OpenLineage**, które zbieram i wizualizuję w **Marquezie**.

Efekt końcowy: klikalny graf pokazujący, skąd dane przyszły, jak były
przekształcane i gdzie wylądowały.

```
raw_sea_ice.csv ──[ingest]──► staging.sea_ice_raw
                                    │
                              [clean]│
                                    ▼
                             staging.sea_ice_clean
                                    │
                          [aggregate]│
                                    ▼
                             marts.sea_ice_monthly
```

---

## Pojęcia (3 klocki OpenLineage)

| Pojęcie | Co to jest | W tym projekcie |
|---|---|---|
| **Job** | krok przetwarzania | `ingest_sea_ice`, `clean_sea_ice`, `aggregate_sea_ice` |
| **Run** | konkretne uruchomienie joba | jedno odpalenie `pipeline.py` (każdy job dostaje swój `runId`) |
| **Dataset** | zbiór danych na wejściu/wyjściu | `raw_sea_ice.csv`, `staging.*`, `marts.*` |

Każdy job emituje dwa **eventy**: `START` (zaczynam) i `COMPLETE` (skończyłem),
mówiąc przy tym, jakie **Datasety** wziął na wejściu i jakie wyprodukował na
wyjściu. Z tych eventów Marquez składa graf. Do datasetów doczepiamy też
**facet `schema`** — listę kolumn z typami.

---

## Wymagania

- **Docker** + **Docker Compose** (do postawienia Marqueza)
- **Python 3.8+**
- Git

---

## Krok 1 — Postaw Marquez (backend + UI)

Marquez to referencyjny serwer OpenLineage. Stawia się jednym skryptem
(odpala w Dockerze bazę Postgres + API + web UI):

```bash
git clone https://github.com/MarquezProject/marquez.git
cd marquez
./docker/up.sh
```

Po chwili:
- **Web UI:** http://localhost:3000
- **API:** http://localhost:5000  ← tu pipeline wysyła eventy

> Najczęstszy problem na tym etapie to zajęte porty albo niewstający Docker.
> Sprawdź to **jako pierwsze**, zanim zabierzesz się za skrypt.

---

## Krok 2 — Środowisko Pythona

```bash
cd ..              # wróć z katalogu marquez do katalogu projektu
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Krok 3 — Uruchom pipeline

**Najpierw test bez Marqueza** (eventy lecą na konsolę — sprawdzasz, że skrypt
w ogóle działa):

```bash
OPENLINEAGE_TRANSPORT=console python pipeline.py
```

**Potem na serio, z Marquezem** (domyślnie wysyła na http://localhost:5000):

```bash
python pipeline.py
```

Oczekiwany wynik:

```
Uruchamiam pipeline z emisją lineage do OpenLineage...
  [ingest_sea_ice] OK
  [clean_sea_ice] OK
  [aggregate_sea_ice] OK
Gotowe. Wynik: 120 wierszy miesięcznych w data/sea_ice_monthly.csv
```

---

## Krok 4 — Zobacz graf

Wejdź na **http://localhost:3000**, w lewym górnym rogu wybierz namespace
**`sea_ice`**. Zobaczysz trzy joby połączone datasetami — to jest Twój lineage.
Kliknij dataset, żeby zobaczyć jego schemat (facet `schema`).

> Tutaj zrób **screenshot** grafu i wrzuć go do README (sekcja niżej) — to
> najmocniejszy element do pokazania na rozmowie.

```
<!-- ![Graf lineage w Marquezie](docs/lineage_graph.png) -->
```

---

## Jak to jest zbudowane (pliki)

- `pipeline.py` — całość: generowanie surowych danych, ETL w pandas
  (`ingest` → `clean` → `aggregate`) oraz emisja eventów OpenLineage.
- `requirements.txt` — zależności (przypięte wersje).
- `data/` — pliki tworzone w trakcie (surowe + pośrednie + wynik).

Logika emisji jest w trzech miejscach `pipeline.py`:
- `build_client()` — wybór transportu (console do testów / http do Marqueza),
- `emit()` — wysyłka pojedynczego `RunEvent`,
- `run_job()` — opakowanie kroku: `START` → praca → `COMPLETE` (lub `FAIL`).

---

## Po co to w ogóle — kontekst (governance)

Data lineage to fundament **data governance**: jak ktoś pyta „skąd ta liczba?”
albo coś się zepsuje w pipeline, lineage pozwala prześledzić całą drogę danych
wstecz — to buduje zaufanie do danych. OpenLineage to **otwarty standard**
opisywania tego, dzięki czemu różne narzędzia (Airflow, dbt, Spark) mogą
raportować lineage w jeden, spójny sposób, a katalogi takie jak **OpenMetadata**
mogą go pokazywać.

---

## Pomysły na rozszerzenie (jeśli będzie czas)

- Podpiąć pipeline pod **Airflow** z providerem OpenLineage (lineage zbierany
  automatycznie z DAG-a) zamiast emisji ręcznej.
- Dodać facet **dataQuality** (np. liczba odrzuconych wierszy w `clean`).
- Dodać prawdziwe dane (np. NSIDC Sea Ice Index) zamiast syntetycznych.
