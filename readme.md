# PEI Group Assessment – PySpark ETL Pipeline

## Project Structure

This repository contains a PySpark-based ETL pipeline developed using Databricks. It processes raw sales, customer, and product datasets, creates enriched and aggregated tables, and includes unit tests to validate the transformations.

### Folder Structure

```text
pei_assessment/
│
├── data/
│   ├── Orders.json
│   ├── Products.csv
│   └── Customer.xlsx
│
├── assessment/
│   ├── transformation.py
│   └── run_pipeline.py
│
├── commons/
│   └── commons.py
│
├── tests/
│   └── test_case.py
│
└── README.md
```

## Folder and File Description

### 1. data/

Contains the source datasets used by the ETL pipeline.

| File | Description |
|---|---|
| `Orders.json` | Raw sales and order transaction data |
| `Products.csv` | Product details, including category, sub-category and pricing |
| `Customer.xlsx` | Customer details, including customer name, segment and country |

### 2. assessment/

Contains the main ETL pipeline implementation.

- **transformation.py** – Main data transformation notebook. Handles data ingestion, raw table creation, data cleansing, customer and product enrichment, order enrichment, and profit aggregations.
- **run_pipeline.py** – Entry point for executing the pipeline and collecting execution results.

### 3. commons/

Contains reusable PySpark functions shared across the project.

**commons.py** includes:

- Column name standardization.
- String trimming and empty-value handling.
- Customer name cleansing.
- Table creation and overwrite functionality.
- SCD Type 2 implementation using Delta Lake MERGE.

### 4. tests/

Contains unit tests for validating the pipeline functionality.

**test_case.py** covers:

- Column name standardization.
- Whitespace trimming.
- Customer name cleansing.
- SCD Type 2 initial loading.
- Reprocessing the same data without creating duplicates.
- Handling changed customer records.
- Inserting new customer records.
- NULL-safe change detection.

## Data Processing Flow

```text
Source Datasets
      |
      v
Data Ingestion
      |
      v
Raw Tables
      |
      v
Data Cleaning and Transformation
      |
      v
Enriched Customer and Product Tables
      |
      v
Enriched Orders Table
      |
      v
Aggregated Profit Table
      |
      v
Business Aggregations
```

## Technologies Used

- Databricks Community Edition
- Python
- PySpark
- Delta Lake
- Spark SQL
- Pandas
- Unit Testing

## Implementation Highlights

- Reusable PySpark transformation functions.
- SCD Type 2 for maintaining historical customer and product records.
- Date-based joins to retrieve historically valid dimension records.
- Broadcast joins for smaller dimension datasets.
- Profit calculations rounded to two decimal places.
- Unit tests covering transformation and historical data scenarios.

## Execution

1. Import or clone the repository into Databricks.
2. Ensure the source files are available in the configured data directory.
3. Configure the data path in `commons/commons.py`.
4. Run `assessment/transformation.py` to execute the ETL pipeline.
5. Run `tests/test_case.py` to validate the transformation logic.

The pipeline creates raw, enriched, and aggregated Delta tables in the `pei_assesment` schema.