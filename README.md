# Real-Time ICU Patient Monitoring Data Platform

![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white)
![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-000000?style=flat-square&logo=microsoft&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=flat-square&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![ADLS Gen2](https://img.shields.io/badge/ADLS_Gen2-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=flat-square&logo=databricks&logoColor=white)

## Overview

This project implements an end-to-end data engineering platform for real-time ICU patient monitoring using Azure and Microsoft Fabric.

The system simulates high-frequency patient vitals, processes data through a Medallion Architecture (Bronze, Silver, Gold), and delivers actionable clinical insights via a Power BI dashboard.

The focus of this project is on designing a scalable, maintainable, and production-aligned data pipeline that reflects real-world healthcare data workflows.

---

## Problem Statement

Healthcare systems generate continuous streams of patient vital data in ICU environments. This data must be ingested, validated, transformed, and made available for downstream analytics with high reliability.

Common challenges include:
- Handling high-frequency streaming data
- Maintaining data quality across multiple transformation layers
- Designing pipelines that are both scalable and maintainable
- Enabling near real-time visibility for clinical decision-making

---

## Solution

This platform simulates ICU patient data and implements a layered data architecture using Azure services and Microsoft Fabric.

The system:
- Ingests streaming and batch data into a Bronze layer
- Applies validation and cleansing in the Silver layer
- Computes clinical metrics and risk scoring in the Gold layer
- Serves curated data through a Lakehouse for analytical consumption

---

## Architecture
```
Microsoft Fabric Pipeline (Scheduled Daily)
        |
01_simulate_new_data — Fabric Notebook
        | 50 ICU patient records (JSON) per run
Bronze Layer — ADLS Gen2 (raw JSON, immutable)
        |
02_bronze_to_silver — Fabric Notebook
        | Data quality checks, schema validation, Parquet
Silver Layer — ADLS Gen2 (cleaned, partitioned Parquet)
        |
03_silver_to_gold — Fabric Notebook
        | NEWS-based risk scoring, aggregations
Gold Layer — ADLS Gen2 (aggregated Parquet)
        |
04_gold_to_lakehouse — Fabric Notebook
        | Delta table writes
Microsoft Fabric Lakehouse (Delta tables)
        |
Direct Lake Semantic Model
        |
Power BI Dashboard — ICU Patient Monitoring
```

This architecture follows the **Medallion pattern** to enforce separation of concerns across ingestion, transformation, and serving layers. Each layer serves a distinct purpose — raw data preservation, quality-assured processing, and business-level aggregation — ensuring full reprocessability and auditability at every stage.

---

## Medallion Architecture — Layer Design

**Bronze Layer**

Stores raw JSON records exactly as generated, with no transformation applied. Acts as the immutable source of truth. Supports full replay and reprocessing from any point in time. Partitioned by year, month, day, and hour.

**Silver Layer**

Applies data quality validation against clinical thresholds. Enforces schema consistency and handles legacy field formats. Removes duplicate and out-of-range records. Outputs clean, partitioned Parquet files ready for analytical processing.

**Gold Layer**

Computes business-level and clinical metrics. Implements NEWS-inspired risk scoring across six vital parameters. Produces three purpose-built datasets: patient risk summary, hourly aggregations by risk level, and ICU-wide summary metrics. Outputs microsecond-precision Parquet for Fabric compatibility.

**Serving Layer**

Loads curated Gold datasets into Microsoft Fabric Lakehouse as Delta tables. Exposes tables via Direct Lake semantic model for zero-copy Power BI consumption. Supports schema evolution via overwriteSchema option.

---

## Pipeline Scripts

| Script | Layer | Description |
|--------|-------|-------------|
| `01_simulate_new_data.py` | Ingestion | Generates 50 realistic ICU patient vitals per run using weighted clinical distributions. Writes JSON records to Bronze layer partitioned by datetime. |
| `02_bronze_to_silver.py` | Processing | Reads latest Bronze partition only. Applies deduplication, timestamp normalization, blood pressure format handling, and clinical range validation across all six vitals. Writes clean Parquet to Silver layer with full overwrite to prevent accumulation. |
| `03_silver_to_gold.py` | Aggregation | Reads latest Silver partition. Computes patient-level risk scores using NEWS-inspired algorithm. Builds three Gold tables: patient risk summary, hourly aggregations by risk level, and ICU summary metrics. |
| `04_gold_to_lakehouse.ipynb` | Serving | Reads Gold Parquet via WASBS protocol with SAS authentication. Writes all three tables to Microsoft Fabric Lakehouse as Delta tables with schema overwrite support. |

---

## Clinical Risk Scoring

Patient risk is computed using a **NEWS (National Early Warning Score)**-inspired algorithm. Each vital parameter is evaluated against clinical thresholds and assigned a weighted score. The aggregate score determines the patient's risk classification.

| Vital Sign | Normal Range | Alert Threshold |
|------------|-------------|-----------------|
| Heart Rate | 60–100 bpm | < 60 or > 100 bpm |
| Oxygen Saturation | 95–100% | < 95% |
| Temperature | 97.0–100.4°F | < 97.0 or > 100.4°F |
| Systolic BP | 90–140 mmHg | < 90 or > 140 mmHg |
| Diastolic BP | 60–90 mmHg | < 60 or > 90 mmHg |
| Respiratory Rate | 12–20 /min | < 12 or > 20 /min |

**Risk Classification:**

| Score | Risk Level |
|-------|-----------|
| 0–1 | Low |
| 2–4 | Medium |
| 5–7 | High |
| 8+ | Critical |

Patients meeting any single alert threshold are flagged independently of their aggregate score, enabling immediate triage prioritization.

---

## Power BI Dashboard

The dashboard is hosted on **Microsoft Fabric** and connected via **Direct Lake mode**, enabling zero-copy querying directly over Delta tables without import or scheduled dataset refresh overhead.

The dashboard refreshes automatically following each pipeline execution via the scheduled Fabric orchestration.

**Dashboard visuals:**

| Visual | Source Table | Metric |
|--------|-------------|--------|
| KPI Card | icu_summary | Total Patients |
| KPI Card | icu_summary | Critical Patient Count |
| KPI Card | icu_summary | Alert Count |
| Donut Chart | patient_risk_summary | Risk Level Distribution |
| Bar Chart | patient_risk_summary | Average Heart Rate and Oxygen Saturation by Risk Level |
| Detail Table | patient_risk_summary | Per-patient vitals with conditional clinical threshold formatting |

Conditional formatting on the detail table uses muted red highlighting for out-of-range values, consistent with clinical dashboard standards.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Simulation | Python |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Python, Pandas, PyArrow |
| Orchestration | Microsoft Fabric Pipeline (scheduled daily) |
| Serving | Microsoft Fabric Lakehouse, Delta Lake |
| Visualization | Power BI (Direct Lake mode) |
| Format | Parquet (Silver, Gold), JSON (Bronze), Delta (Lakehouse) |
| CI/CD | Azure DevOps (coming soon) |

---

## How to Run

**Clone the repository**
```bash
git clone https://github.com/Syed-Shoaib08/azure-healthcare-icu-platform.git
```

**Install dependencies**
```bash
pip install azure-storage-blob pandas pyarrow faker
```

**Configure credentials** — replace placeholders in each script:
```python
STORAGE_CONNECTION_STR = "YOUR_AZURE_STORAGE_CONNECTION_STRING"
```

**Run pipeline in order:**
```bash
python 01_simulate_new_data.py
python 02_bronze_to_silver.py
python 03_silver_to_gold.py
# Run 04_gold_to_lakehouse.ipynb in Microsoft Fabric
```

---

## Project Structure
```
azure-healthcare-icu-platform/
├── 01_simulate_new_data.py       # Data generation — weighted clinical distributions
├── 02_bronze_to_silver.py        # Bronze to Silver — quality validation and Parquet
├── 03_silver_to_gold.py          # Silver to Gold — NEWS risk scoring and aggregation
├── 04_gold_to_lakehouse.ipynb    # Gold to Lakehouse — Delta table writes via Fabric
├── assets/                       # Architecture screenshots and dashboard visuals
└── README.md
```

## Key Learnings

- Designing Medallion Architecture for real-world use cases
- Handling streaming and batch data in a unified pipeline
- Implementing data validation and quality checks
- Translating raw data into meaningful business insights
- Structuring pipelines for maintainability and scalability

---

## Security

All credentials and connection strings have been removed from this repository. Azure storage keys, SAS tokens, and connection strings must be supplied via environment variables or **Azure Key Vault** before running any script. Never commit credentials to version control.

---

## Screenshots

### Pipeline Execution — All 4 Notebooks Succeeded
![Pipeline Success](assets/01_pipeline_success.png)

### Silver to Gold — Clinical Risk Scoring Output
![Silver to Gold Output](assets/02_silver_to_gold_output.png)

### Power BI Dashboard — ICU Patient Monitoring
![Power BI Dashboard](assets/03_powerbi_dashboard.png)

### Microsoft Fabric Lakehouse — Delta Tables
![Lakehouse Tables](assets/04_lakehouse_tables.png)

### Azure ADLS Gen2 — Medallion Architecture Storage
![ADLS Containers](assets/05_adls_containers.png)
