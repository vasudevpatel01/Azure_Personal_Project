## Incremental Ingestion + Databricks Transformations (Medallion Architecture)

Project purpose:
Build a production-ready, reusable incremental ingestion pipeline (ADF) that lands data to ADLS Gen2 (bronze), transform it in Databricks (silver), and produce SCD Type 2 gold tables (DLT) — designed for reliable, auditable, query-ready datasets

![Screenshot_25-10-2025_122546_adf azure com](https://github.com/user-attachments/assets/f45421a9-fdf7-432b-bed4-1ae4f6318371)


### Project summary

This project demonstrates an end-to-end medallion pipeline built for incremental ingestion and reliable transformations:

Ingest: Azure Data Factory (ADF) copies only changed rows from Azure SQL Database to ADLS Gen2 (bronze) using a small cdc.json watermark per table.

Transform: Databricks AutoLoader reads parquet files from bronze, applies transformations and deduplication to produce silver Delta tables.

Enrich / Historicize: Delta Live Tables (or DLT-style auto CDC flows) create SCD Type 2 gold tables that preserve historical changes for downstream BI.

Automation: Conditional logic, metadata updates, checkpointing, and a Web activity enable end-to-end automation.

```
repo-root/
├─ Spotify_dab/                                   # main project folder 
│  ├─ databricks_pipleine_integrated/             # Databricks code & DLT 
│  │  ├─ notebooks/
│  │  │  └─ silver_dim.ipynb                       # silver layer notebook 
│  │  ├─ dlt/
│  │  │  └─ cdc_pipelines.py                       # DLT / create_auto_cdc_flow scripts
│  │  ├─ libs/
│  │  │  └─ utils/
│  │  │     └─ transform.py                        # helper transform functions
│  │  ├─ jobs/                                     # Databricks job JSONs
│  │  └─ requirements.txt                        
│  │
│  ├─ dataset/                                     # dataset-related assets
│  │  └─ Parquet_Dynamic/                          # dataset reference used by ADF sink
│  │
│  ├─ factory/
│  │  └─ df-001azureproject/                       # ADF factory artifact folder 
│  │     ├─ pipelines/
│  │     │  └─ incremental_ingestion.json          # **ADF pipeline JSON **
│  │     ├─ datasets/                              # ADF dataset JSONs (AzureSQL, Parquet_Dynamic, Json_Dynamic)
│  │     │  ├─ AzureSQL.json
│  │     │  ├─ Parquet_Dynamic.json
│  │     │  └─ Json_Dynamic.json
│  │     └─ linkedServices/                         # factory-local linked service definitions
│  │        └─ AzureDataLakeStorage1.json
│  │
│  ├─ linkedService/
│  │  └─ AzureDataLakeStorage/                     # linked service folder
│  │     └─ README.md                              # notes about permissions / service principal
│  │
│  ├─ pipeline/
│  │  └─ incremental_ingestion/                    # pipeline-specific assets
│  │     ├─ incremental_ingestion.json             # copy of pipeline definition 
│  │     └─ publish_config.json                     # ADF publish config / infra metadata
│  │
│  ├─ configs/
│  │  ├─ empty.json                                # template used by update_last_cdc
│  │
│  ├─ README.md                                    # **Project README **
│  └─ publish_config.json                          # top-level copy 
│
└─ .gitignore

```


Discovery & parameters: The ADF pipeline accepts schema, table, cdc_col, and optionally from_date. Each run uses these params to drive extraction logic.

Read last watermark: A Lookup reads the cdc.json stored under bronze/<table>_cdc/cdc.json to learn the last successfully processed watermark.

Set run id: A SetVariable captures a current timestamp (e.g., utcNow()) used to form a unique parquet filename: <table>_<current>.parquet.

Incremental extract: A Copy activity runs a parameterized query against Azure SQL to select records where cdc_col > last_watermark (or from_date if provided) and writes Parquet to bronze/<table>/.

Conditional check: An IfCondition evaluates whether the Copy activity returned rows (dataread > 0).

If no rows, delete the empty parquet file to avoid noise.

If rows exist, run max_cdc to compute the new high watermark and update_last_cdc to overwrite cdc.json with the new value.

Databricks ingestion: Databricks AutoLoader (cloudFiles) picks up the new parquet(s) from bronze/<table> using cloudFiles.schemaLocation check-pointing and loads them as streaming micro-batches.

Transform & Silver writes: Notebooks apply transformations (cleanup, dedupe, enrichment, derived columns) and write the results as Delta tables in the silver container and register tables in your metastore (e.g., spotify_cat.silver.<table>). Each write uses a table-specific checkpoint and .trigger(once=True) to process available files.

Gold / SCD Type 2: DLT or helper create_auto_cdc_flow constructs SCD Type 2 flows for each silver table, generating gold tables that store historical rows and versioning metadata.

Downstream automation: Optionally call a Web activity to inform downstream systems or kick off other jobs after successful pipeline completion.

### ADF pipeline — details & parameters

Pipeline name: incremental_ingestion (example)
Parameters:

schema (string) — e.g., dbo

table (string) — e.g., DimUser

cdc_col (string) — e.g., last_modified_ts or an integer sequence column

from_date (string, optional) — override watermark on first-run or reprocessing

Important activities:

last_cdc (Lookup): reads bronze/{table}_cdc/cdc.json.

current (SetVariable): sets current = utcNow().

AzureSQLtoLake (Copy): copies rows > watermark to Parquet_Dynamic dataset. Uses TabularTranslator for type handling.

IfIncrementalData (IfCondition): checks @greater(activity('AzureSQLtoLake').output.dataread,0).

DeleteEmptyFile (Delete) if zero rows.

max_cdc (Script) + update_last_cdc (Copy) if rows processed.

### Transform & write:

Apply data cleaning and business logic (e.g., uppercase names, classify fields, derive start_year, drop _rescued_data).

Use .writeStream.format("delta") with .option("checkpointLocation", <checkpoint>) and .toTable("spotify_cat.silver.<Table>") plus .trigger(once=True).

*Note* : the repo includes create_auto_cdc_flow usage for all tables to produce SCD Type 2 gold tables.


### How to run — quick start

Pre-requisites

Azure subscription, ADLS Gen2 (bronze, silver containers).

ADF with linked services: Azure SQL & ADLS Gen2.

Databricks workspace, cluster or job cluster with access to ADLS (service principal or managed identity).

Databricks Repos or workspace path for notebooks & libs.

Deploy ADF

Import adf/pipelines/incremental_ingestion.json in Azure Data Factory UI (or deploy via ARM).

Create datasets: AzureSQL, Parquet_Dynamic, Json_Dynamic. Parameterize container/folder/file names as in pipeline.

Bootstrap cdc.json

Upload configs/empty.json as bronze/{table}_cdc/empty.json.

Optionally create an initial cdc.json with a sensible start (or let pipeline create/overwrite on first successful run).

Set pipeline parameters & trigger

Example parameters:

schema = dbo

table = DimUser

cdc_col = last_modified_ts

from_date = (leave empty to use cdc.json)

Run pipeline manually or via scheduled trigger.

Databricks

Place notebooks in Databricks Repos and ensure sys.path or repo path references are correct (e.g., sys.path.append('/Workspace/Users/.../spotify_dab')).

Create Databricks job(s) for notebooks or DLT pipelines. Trigger them after ADF completes (or use a Web activity).

Validate

Check bronze for parquet files, bronze/<table>_cdc/cdc.json for watermark updates.

Query silver: SELECT COUNT(*) FROM spotify_cat.silver.<Table>;

Query gold SCD tables for effective_from / effective_to or versioning system columns added by DLT.
