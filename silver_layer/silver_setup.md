# Silver Layer Overview

## Key Function

The Silver Layer is responsible for refining raw data ingested from the Bronze Layer into clean, structured, and governed tables.

## Description

This layer is built entirely on Azure Databricks and features two key components:

  - **Data Governance** : Implemented using Unity Catalog (including metastores, credentials, and external locations) for centralized access control and enhanced security.

  - **Data Processing** : Utilizes Spark Structured Streaming with Auto Loader to create robust, fault-tolerant, and metadriven ingestion pipelines that automatically handle schema evolution and incremental updates.

## Unity Catalog Security Configuration

To enable Unity Catalog to access data in the Azure Data Lake, a secure, role-based access method was implemented using an Azure Managed Identity and an Access Connector.

1. Unity Metastore and Storage Account
  - The foundation of governance begins with the Unity Metastore, which stores all metadata and access rules.

  - A Storage Account was created to act as the root storage for the Metastore's system and governance data.

![Creating Metastore](../images/creatingMetastore.PNG)

2. Creating the Access Connector
  - The Access Connector acts as the secure intermediary between Databricks and the Data Lake.

  - A dedicated Azure resource, named `accessazureproject` (a Databricks Access Connector), was created.This resource automatically provisions an Azure Managed Identity.

![Access Connector](../images/accessConnector.png)

3. Granting Data Lake Access (Role Assignment)
  - The Managed Identity created needs specific permission on the Data Lake to read the Bronze layer and write to the Silver layer.

  - Target Resource: ADLS Gen2 account.

  - Role Assignment: The Managed Identity associated with the `accessazureproject` connector was granted the Storage Blob Data Contributor role.

  - Result: This role assignment allows the Managed Identity to read, write, and delete blobs within the data lake, securely granting Databricks access to the Bronze data.


## Unity Catalog Structure and Data Mapping

To establish proper data governance and separation of environments, a structured architecture was implemented in Unity Catalog, mapping logical catalogs and schemas to the physical storage containers in the Data Lake.

1. Catalog and Schema Definition

### Unity Catalog Components

| Component | Name | Purpose |
|------------|------|----------|
| **Catalog** | `spotify_cata` | The top-level logical container for the entire Spotify data domain. |
| **Schema** | `silver` | The specific logical container within the catalog for holding all clean, Silver Layer tables. |

2. External Credential
The security configuration established previously is formalized in Databricks as an External Credential.

- Credential Name: credential (This represents the `accessazureproject` Managed Identity Access Connector).

- Function: This credential allows Unity Catalog to securely assume the Managed Identity's permissions when accessing the Data Lake.

3. External Locations

External Locations map the secured credential to specific physical file paths in the Data Lake. These locations define the only permitted read/write paths for data processing.

| External Location | Purpose |
|------------|------|
| `bronze` | Read-only path for accessing raw data ingested by ADF. |
| `silver` | Write-path for storing the cleaned, refined Delta tables. |
| `gold` | Write-path for storing final aggregated reporting tables. |

4. Notebook Development Environment
- Workspace Folder: `SpotifyAzureProject`

- Processing: A dedicated notebook within this folder handles the dimension and fact table processing.

- Execution: The notebook is attached to a Serverless Cluster, utilizing Databricks' optimized and automatically managed compute environment for cost-efficient processing.












