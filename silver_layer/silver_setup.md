# Silver Layer Overview

## Key Function

The Silver Layer is responsible for refining raw data ingested from the Bronze Layer into clean, structured, and governed tables.

## Description

This layer is built entirely on Azure Databricks and features two key components:

- **Data Governance** : Implemented using Unity Catalog (including metastores, credentials, and external locations) for centralized access control and enhanced security.

- **Data Processing** : Utilizes Spark Structured Streaming with Auto Loader to create robust, fault-tolerant, and metadriven ingestion pipelines that automatically handle schema evolution and incremental updates.
