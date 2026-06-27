# Sea Ice Data Lineage — OpenLineage + Marquez

Demonstracja **end-to-end data lineage** na pipelinie ETL: dane przechodzą przez
kolejne transformacje, a każdy krok automatycznie raportuje swoje pochodzenie
(źródła, wyjścia, schematy). Efektem jest klikalny graf pokazujący pełną drogę
danych — od surowego pliku po zagregowaną tabelę wynikową.

Projekt łączy trzy obszary:
(pipeline ETL), metadane (lineage + schematy) i obserwowalność (śledzenie
uruchomień i ich statusów).

![Graf lineage w Marquezie](https://github.com/user-attachments/assets/5b5b6555-f975-42b1-8be3-5475e3a35a48)

*Graf lineage w Marquezie: trzy kroki (`ingest → clean → aggregate`) połączone
datasetami, ze schematem każdej tabeli i jego ewolucją wzdłuż pipeline'u.*

---

## Co projekt demonstruje

- **Instrumentację pipeline'u standardem OpenLineage** — ręczna emisja zdarzeń
  `START` / `COMPLETE` / `FAIL` z poziomu kodu, z poprawnym modelem Job / Run / Dataset.
- **Modelowanie metadanych** — przepływ danych opisany jako relacje wejście→wyjście,
  ze schematami kolumn (facet `schema`) i ich ewolucją na kolejnych etapach.
- **ETL z naciskiem na jakość danych** — czyszczenie braków i wartości odstających,
  typowanie, agregacja (pandas).
- **Pracę z infrastrukturą w Dockerze** — lokalne uruchomienie backendu lineage
  (Marquez: API + PostgreSQL + UI + OpenSearch) i diagnostyka z logów.

---

## Architektura

```
  pipeline.py ──(zdarzenia OpenLineage / HTTP)──►  Marquez API ──►  PostgreSQL
       │                                                │
       │ produkuje pliki danych                    Marquez Web UI  (graf lineage)
       ▼
   data/*.csv, *.parquet
```

Pipeline emituje metadane *o* przepływie danych do API Marqueza; same dane
(CSV/parquet) zapisuje na dysk. Marquez gromadzi zdarzenia i wizualizuje je jako graf.

**Pipeline (3 kroki):**

```
raw_sea_ice.csv ──[ingest]──► staging.sea_ice_raw ──[clean]──► staging.sea_ice_clean ──[aggregate]──► marts.sea_ice_monthly
```

---

## Model lineage (OpenLineage)

| Pojęcie | Znaczenie | W projekcie |
|---|---|---|
| **Job** | krok przetwarzania | `ingest_sea_ice`, `clean_sea_ice`, `aggregate_sea_ice` |
| **Run** | uruchomienie joba | jedno odpalenie pipeline'u (osobny `runId` na krok) |
| **Dataset** | dane wej./wyj. | `raw_sea_ice.csv`, `staging.*`, `marts.*` |
| **Facet** | metadane datasetu | `schema` — kolumny i typy |

Każdy krok raportuje, jakie datasety wziął na wejściu i jakie wyprodukował na
wyjściu — z tych relacji Marquez odtwarza graf.

---

## Stack technologiczny

**Python** (pandas, NumPy) · **OpenLineage** (`openlineage-python`) ·
**Marquez** · **PostgreSQL** · **Docker / Docker Compose**

---

## Struktura projektu

| Plik | Zawartość |
|---|---|
| `pipeline.py` | Pipeline ETL + emisja zdarzeń OpenLineage |
| `requirements.txt` | Zależności (przypięte wersje) |
| `NOTATKI_konfiguracja.md` | Dokumentacja konfiguracji i rozwiązanych problemów |
| `WYJASNIENIE_kodu.md` | Omówienie kodu sekcja po sekcji |

---

## Uruchomienie

```bash
# 1. Backend lineage (Marquez w Dockerze)
git clone https://github.com/MarquezProject/marquez.git
cd marquez && ./docker/up.sh          # UI: localhost:3000 · API: localhost:5000

# 2. Pipeline
pip install -r requirements.txt
python pipeline.py                    # wysyła lineage do Marqueza
```

Tryb testowy bez Marqueza (zdarzenia na konsolę): `OPENLINEAGE_TRANSPORT=console python pipeline.py`.
Graf: `localhost:3000` → namespace `sea_ice`.

---

## Możliwe rozszerzenia

- Automatyczne zbieranie lineage przez **Airflow** + provider OpenLineage (zamiast emisji ręcznej).
- Facet **dataQuality** (np. liczba odrzuconych wierszy na etapie czyszczenia).
- Realne dane (NSIDC Sea Ice Index) zamiast syntetycznych.
- Onboarding metadanych do **OpenMetadata** jako katalogu danych.
