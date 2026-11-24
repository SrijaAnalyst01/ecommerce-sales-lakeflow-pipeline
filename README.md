# E-commerce-sales-lakeflow-pipeline

# End-to-End E-Commerce Sales Data Engineering Pipeline (Medallion Architecture)

<img width="790" height="273" alt="DAG" src="https://github.com/user-attachments/assets/37a8c6ee-8ab0-4efb-ac44-70015aa06e31" />

An end-to-end incremental data engineering pipeline built on **Databricks** using **Lakeflow Spark Declarative Pipelines (SDP)**, **PySpark**, **Auto Loader**, and **Unity Catalog**. The pipeline ingests raw daily e-commerce order CSV exports, enforces data quality rules and business-grain deduplication, and materializes business-ready Gold analytical views.

---

## Architecture Overview

```text
Unity Catalog Volume (/Volumes/ecommerce_sdp/ecommerce_data/landing)
        │
        ▼  [Auto Loader / cloudFiles]
┌───────────────────────────────┐
│ BRONZE: bronze_orders_sdp     │  Raw ingestion + audit metadata (2,000 rows)
└───────────────┬───────────────┘
                │
                ▼  [Trim, Standardize, Blank-to-NULL, Safe Type Cast]
┌───────────────────────────────┐
│ SILVER STAGING                │  silver_orders_cleaned ──▶ silver_orders_typed
└───────────────┬───────────────┘
                │
                ▼  [Rules R1–R6 + Business-Grain Deduplication]
       ┌────────┴────────┐
       ▼                 ▼
silver_orders    silver_orders_rejected
(1,735 valid)    (247 rejected + 18 duplicates removed)
       │
       ├──▶ gold_order_summary       (Lifecycle status distribution across all 1,735 orders)
       │
       ▼  [Filter: Delivered Orders Only]
gold_delivered_orders (1,190 unique delivered orders)
       │
       ├──▶ gold_city_sales          (City revenue, AOV & dense_rank)
       ├──▶ gold_category_sales      (Category volume, revenue & rank)
       ├──▶ gold_product_sales       (Product ASP, units sold & rank)
       ├──▶ gold_payment_analysis    (Payment mode share, ATV & rank)
       └──▶ gold_sales_summary       (Executive KPIs)
```

---

## Tech Stack
* **Platform:** Databricks (Unity Catalog & Serverless Compute)
* **Orchestration & Declarative ETL:** Lakeflow Spark Declarative Pipelines (SDP)
* **Processing Engine:** Apache Spark / PySpark
* **Ingestion:** Databricks Auto Loader (`cloudFiles`)
* **Storage:** Managed Delta Tables & Materialized Views

---

## Repository Structure

```text
├── README.md
├── dag.png
├── transformations/
│   ├── bronze_orders_sdp.py         # Auto Loader raw ingestion with metadata lineage
│   ├── silver_orders_cleaned.py     # Whitespace normalization & text standardization
│   ├── silver_orders_typed.py       # Blank-to-NULL handling, multi-format date parsing & try_cast
│   ├── silver_orders.py             # Business rules (R1–R6), quarantine split & deduplication
│   ├── gold_delivered_orders.py     # Streaming filter for Delivered orders
│   ├── gold_city_sales.py           # City-level revenue, AOV & ranking
│   ├── gold_category_sales.py       # Category-level sales, units & ranking
│   ├── gold_product_sales.py        # Product-level units, ASP & ranking
│   ├── gold_payment_analysis.py     # Payment mode transaction & revenue analysis
│   ├── gold_order_summary.py        # Order lifecycle distribution across all valid statuses
│   └── gold_sales_summary.py        # Overall executive KPI summary
└── gold_validation_ecommerce.py     # End-to-end row count, grain & revenue reconciliation checks
```

---

## Key Data Engineering Features

### 1. Bronze Layer (`bronze_orders_sdp`)
* Incrementally ingests raw CSV files from a Unity Catalog landing volume using **Auto Loader (`cloudFiles`)**.
* Preserves all source fields as strings (`inferColumnTypes=false`) to prevent ingestion failures on malformed data.
* Appends `ingestion_timestamp` and `source_file_name` (`_metadata.file_path`) for full data lineage and auditability.

### 2. Silver Layer (`silver_orders_cleaned` → `silver_orders_typed` → `silver_orders` & `silver_orders_rejected`)
* **Standardization:** Trims leading/trailing spaces, collapses repeated whitespace, and standardizes casing (`initcap` for descriptive fields, `upper` for `payment_mode`).
* **Safe Type Conversion:** Normalizes empty strings to `NULL`, parses mixed date formats (`dd/MM/yyyy` and `yyyy-MM-dd`), casts numeric columns safely using `try_cast`, and derives `order_amount` (`quantity * unit_price`).
* **Data Quality & Quarantine Pattern:** Evaluates **6 business validation rules (`R1–R6`)** checking for missing keys, invalid dates, non-positive quantities/prices, missing dimensions, and valid order statuses. Failing rows are routed to `silver_orders_rejected` with concatenated failure reasons rather than silently dropped.
* **Business-Grain Deduplication:** Eliminates exact duplicate records across 11 core business attributes while preserving legitimate multi-line orders.

### 3. Gold Layer (Operational & Revenue Analytics)
* **Operational Lifecycle (`gold_order_summary`):** Reads all `1,735` valid Silver records to track status distribution (`Delivered`, `Shipped`, `Cancelled`, `Returned`) and operational percentages.
* **Realized Revenue Analytics (`gold_delivered_orders` + 5 Materialized Views):** Isolates `Delivered` orders (`1,190` unique orders) so cancelled, returned, and pending orders never inflate sales metrics, computing `dense_rank()` leaderboards and executive KPIs.

---

## Data Quality & Reconciliation Results

Automated validation checks confirm 100% record accountability from raw ingestion through Gold aggregations:

| Layer / Metric | Record Count | Notes |
| :--- | :---: | :--- |
| **Bronze Raw Ingested** | 2,000 | 100% reconciled with source CSV |
| **Silver Valid (Deduplicated)** | 1,735 | Passed all 6 validation rules (`R1–R6`) |
| **Silver Rejected (Quarantined)** | 247 | Flagged with explicit `R1–R6` rejection reasons |
| **Exact Duplicates Removed** | 18 | Deduplicated across 11 business attributes |
| **Delivered Orders (Gold)** | 1,190 | 1:1 unique `order_id` parity; 3,605 total units sold |

### Order Status Breakdown (`gold_order_summary`)
* **Delivered:** 1,190 orders (68.59%) — 3,605 units
* **Shipped:** 276 orders (15.91%) — 817 units
* **Cancelled:** 174 orders (10.03%) — 522 units
* **Returned:** 95 orders (5.48%) — 281 units

---

## How to Run
1. Upload raw order CSV files to `/Volumes/ecommerce_sdp/ecommerce_data/landing`.
2. Configure a **Lakeflow Spark Declarative Pipeline** targeting catalog `ecommerce_sdp` and schema `default`, pointing to the `transformations/` folder.
3. Click **Run pipeline** to execute the Bronze → Silver → Gold DAG.
4. Run `gold_validation_ecommerce.py` in a standalone Databricks notebook to verify row-count reconciliation and dimensional revenue totals.

## BI Dashboard Preview
<img width="842" height="547" alt="image" src="https://github.com/user-attachments/assets/c04a2462-7446-414a-b978-0d8c28cec47e" />
<img width="843" height="556" alt="image" src="https://github.com/user-attachments/assets/fade4f16-11d6-4060-b339-c48e83f51e49" />
<img width="836" height="553" alt="image" src="https://github.com/user-attachments/assets/8c51d7cd-61a7-4afd-9cc9-6467f1f53700" />
<img width="845" height="567" alt="image" src="https://github.com/user-attachments/assets/67482ede-1f8b-4a43-9fce-56079b16440a" />


