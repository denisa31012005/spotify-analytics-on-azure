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

## Azure Data Factory (ADF) Setup
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
The database contains:
- Four dimension tables: `DimUser`, `DimTrack`, `DimDate`, `DimArtist`
- One fact table: `FactStream`

These tables store subscription and streaming information, which form the foundation of the analytical model.

📸 *Screenshot placeholder:*  
`![Azure SQL Database](./pipeline_screenshots/sql_database.png)`

---

## 🔗 Step 5: Linked Services Configuration
To enable data movement between ADF and the other Azure services, I configured two **Linked Services**:

1. **Azure SQL Database Linked Service**
   - Allows ADF to read data from the SQL source.
   - Uses connection string and authentication via Managed Identity or Key Vault.

2. **Azure Data Lake Linked Service**
   - Enables ADF to write ingested data into the Data Lake’s Bronze container.
   - Configured using Azure Active Directory (AAD) or account key authentication.

📸 *Screenshot placeholder:*  
`![Linked Services](./pipeline_screenshots/linked_services.png)`

---

## 🔄 Step 6: Dynamic Incremental and Backfill Pipeline
I built an ADF **pipeline** to perform both **initial backfill** and **incremental loading** from Azure SQL Database into the Bronze layer.

### Pipeline Logic:
- Uses **parameters** for:
  - Table name  
  - Destination path  
  - Last load date (watermark)
- Executes a **dynamic SQL query** in the ADF copy activity to pull only new or updated records.
- Writes the output into the **Bronze container** in **Parquet** or **CSV** format.
- Supports reusability for multiple tables through metadata-driven configuration.

📸 *Screenshot placeholder:*  
`![ADF Pipeline](./pipeline_screenshots/adf_pipeline_overview.png)`

---

## ⚡ Step 7: Data Validation
After the first pipeline execution:
- Verified the ingested files in the `bronze/` container.
- Confirmed schema consistency with source tables.
- Validated incremental runs by comparing row counts and timestamps.

📸 *Screenshot placeholder:*  
`![Bronze Data Validation](./pipeline_screenshots/bronze_data_validation.png)`

---

## ✅ Outcome
The **Bronze layer** is now fully configured and automated to:
- Continuously capture raw data from Azure SQL Database.
- Perform both **historical backfill** and **incremental loading** dynamically.
- Store all data in **Azure Data Lake (Bronze container)** as the foundation for downstream Silver and Gold transformations.
