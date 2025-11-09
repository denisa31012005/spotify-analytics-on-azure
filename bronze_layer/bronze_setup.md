# Data Ingestion Setup

## Overview
The **Bronze layer** is responsible for ingesting raw data from the **Azure SQL Database** into **Azure Data Lake Storage Gen2**.  
This layer ensures that data is captured in its original form and supports both **initial backfilling** and **incremental loading** for optimized performance and storage usage.

---

## Resource Group Creation
To organize all related Azure services, I first created a **Resource Group** dedicated to this project.

- it manages services like the Data Lake, SQL Database, and Data Factory.

---

## Azure Data Lake Setup
Within the Resource Group, I created an **Azure Data Lake Storage Gen2** account.  
Inside this storage account, I defined **three containers** to implement the Medallion architecture:

| Container | Purpose |
|------------|----------|
| **bronze** | Stores raw ingested data directly from the source |
| **silver** | Contains cleaned and processed data |
| **gold**   | Holds curated and analytics-ready data models |

---

## Azure Data Factory Setup
Next, I created an **Azure Data Factory** instance in the same Resource Group.  
ADF is used to orchestrate and automate the data movement between the SQL source and the Data Lake.

Main components configured:
- **Pipelines:** Define the data movement logic (backfill + incremental).
- **Linked Services:** Connect ADF securely to source and destination systems.
- **Datasets:** Represent input/output data locations.
- **Parameters:** Enable dynamic, metadata-driven pipeline execution.

---

## Azure SQL Database & Server Setup
Created an **Azure SQL Database** and an associated **SQL Server** to host my source data.  

To initialize the database schema, I executed the `initial_load.sql` script (available in this repo) which creates the necessary tables, including:  
- Four dimension tables: `DimUser`, `DimTrack`, `DimDate`, `DimArtist`
- One fact table: `FactStream`

These tables store subscription and streaming information, which form the foundation of the analytical model.

![Azure SQL Database Setup](../images/creatingSource.PNG)

---

## Linked Services Configuration
To enable data movement between ADF and the other Azure services, I configured two **Linked Services**:

1. **Azure SQL Database Linked Service**
   - Allows ADF to read data from the SQL source.

2. **Azure Data Lake Linked Service**
   - Enables ADF to write ingested data into the Data Lake’s Bronze container.

---

## Dynamic Incremental and Backfill Pipeline
I built an ADF **pipeline** to perform both **initial backfill** and **incremental loading** from Azure SQL Database into the Bronze layer.

## Incremental Ingestion Pipeline Design

The **Incremental_Ingestion** pipeline is designed to move data from the Azure SQL Database to the Data Lake's bronze container, efficiently handling both the initial full load and subsequent incremental (CDC - Change Data Capture) loads.  
The pipeline is fully metadata-driven using parameters, making it reusable for any dimension or fact table in the source database.

---

## CDC Metadata Setup

To enable incremental data ingestion, I first created a **CDC (Change Data Capture) tracking file** named `cdc.json`.  
This file maintains the last processed timestamp, ensuring that each subsequent pipeline run only ingests new or updated records.

### creating the cdc.json File

- I created a JSON file called `cdc.json` containing key-value pairs to store the CDC state.
- The initial value is set to a minimal date to perform a **full historical load** on the first pipeline execution:

  ```json
  {"cdc": "1900-01-01"}

### Defining the Initial Load Query

After creating the cdc.json file, I wrote an SQL query to fetch all data from the Azure SQL Database during the first full load.
The query dynamically filters data based on the CDC value from the JSON file.

![Metadata configuration](../images/creatingSource.PNG)

### Pipeline Parameters:
The pipeline uses dynamic parameters to avoid hardcoding table names:
- `@schema`: The source table schema name (e.g., `dbo`).
- `@table`: The source table name (e.g., `DimUser`).
- `@cdc_col`: The name of the CDC column (e.g., `updated_at`).

---

## Pipeline Activities

The pipeline orchestrates five main activities, as shown in the run detail:

| Activity Name  | Type          | Purpose                                                                                   |
|----------------|---------------|-------------------------------------------------------------------------------------------|
| `last_cdc`     | Lookup        | Fetches the last processed timestamp from the `cdc.json` metadata file in the Data Lake. This value is used as the `@from_date` to filter the SQL source. |
| `current`      | Set Variable  | Records the current pipeline execution timestamp. This value will be used as the `@to_date` filter for the current run and as the new CDC value for the next run. |
| `AzureSQLToLake` | Copy Data   | The main ingestion activity. Uses a dynamic SQL query to select all records from the source table where the `@cdc_col` is between the last CDC value and the current timestamp. Writes the output to a dynamic Parquet dataset (`parquet_dynamic`) in the bronze container. |
| `max_cdc`      | Script        | Executes a SQL query against the source database to confirm the actual maximum CDC value successfully read from the table during this run. Ensures the next run starts from the correct point. |
| `update_last_cdc` | Copy Data  | Updates the `cdc.json` file in the Data Lake with the confirmed max timestamp retrieved by the `max_cdc` script, preparing the metadata for the next incremental run. |

---

## Conditional Logic (Mentioned in Setup)

- **Conditional Triggering:**  
  Although not visible in the screenshot, the presence of an `ifNewRecords` condition is crucial.  
  This typically wraps the final `update_last_cdc` activity to ensure the `cdc.json` file is only updated if new records were actually processed, preventing unnecessary updates or logic errors on runs with no new data.

- **Backfilling:**  
  The initial full load is naturally handled by setting the starting `cdc.json` value to `1900-01-01`, which satisfies the backfilling requirement by reading all historical data.

---
