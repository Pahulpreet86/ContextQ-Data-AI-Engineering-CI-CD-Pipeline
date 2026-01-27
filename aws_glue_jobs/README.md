# AWS Glue Job Parameters & Configuration Guide

## silver_layer_transformation_glue.py
### Required Job Parameters
The following parameters must be provided when running the Glue job.
| Parameter                 | Description                                         | Value                                                     |
| ------------------------- | --------------------------------------------------- | --------------------------------------------------------- |
| `--BRONZE_COMPANIES_PATH` | S3 path to raw companies data (Bronze layer)        | `s3://acme-dev-datalake/bronze/companies_raw`                |
| `--BRONZE_TECH_PATH`      | S3 path to raw tech companies data (Bronze layer)   | `s3://acme-dev-datalake/bronze/technology_companies_raw/`           |
| `--FUZZY_OUTPUT_PATH`     | S3 path for fuzzy matching results                  | `s3://acme-dev-datalake/silver/matching/fuzzy_match_output/` |
| `--SILVER_COMPANIES_PATH` | S3 path for processed companies data (Silver layer) | `s3://acme-dev-datalake/silver/company_registry/`                |
| `--SILVER_TECH_PATH`      | S3 path for processed tech data (Silver layer)      | `s3://acme-dev-datalake/silver/tech_companies/`           |


## graphframe_corporate_id_glue.py
### Required Job Parameters
The following parameters must be provided when running the Glue job.
| Parameter                     | Description                                                               | Value                                                                |
| ----------------------------- | ------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `--SILVER_COMPANIES_PATH`     | S3 path to Silver-layer companies dataset                                 | `s3://acme-dev-datalake/silver/company_registry/`                    |
| `--SILVER_TECH_PATH`          | S3 path to Silver-layer tech companies dataset                            | `s3://acme-dev-datalake/silver/tech_companies/`                      |
| `--FUZZY_MATCHES_PATH`        | S3 path to fuzzy matching results (from Silver layer)                     | `s3://acme-dev-datalake/silver/matching/fuzzy_match_output/`         |
| `--GOLDEN_CORPORATE_REGISTRY` | S3 output path for Golden corporate registry                              | `s3://acme-dev-datalake/golden/corporate_registry/`                  |
| `--CHECKPOINT_PATH`           | S3 path for GraphFrames checkpointing (required for connected components) | `s3://acme-dev-datalake/checkpoints/graphframes/corporate_registry/` |


## iceberg-upsert.py
### Required Job Parameters
The following parameters must be provided when running the Glue job.
| Parameter            | Description                                                 | Value                       |
| -------------------- | ----------------------------------------------------------- | ----------------------------------- |
| `--CATALOG_NAME`     | Spark / Iceberg catalog name (Glue-backed)                  | `awsdatacatalog`                      |
| `--DATA_LAKE_BUCKET` | S3 bucket where staging data is stored                      | `acme-dev-datalake`                 |
| `--DATABASE`         | Glue database containing the Iceberg table                  | `acme_datalake`                            |
| `--WAREHOUSE_PATH`   | S3 path for Iceberg warehouse (table storage location)      | `s3://acme-dev-datalake/iceberg/`   |
| `--STAGING_PREFIX`   | S3 prefix containing staging Parquet files (no bucket name) | `golden/corporate_registry/` |
| `--TABLE`            | Target Iceberg table name                                   | `corporate_registry`                |



## iceberg-create.py
### Required Job Parameters
The following parameters must be provided when running the Glue job.
| Parameter            | Description                                                            | Value                                        |
| -------------------- | ---------------------------------------------------------------------- | ---------------------------------------------------- |
| `--CATALOG_NAME`     | Spark / Iceberg catalog name backed by AWS Glue                        | `awsdatacatalog`                                       |
| `--DATABASE`         | AWS Glue database containing Iceberg tables                            | `acme_datalake`                                             |
| `--WAREHOUSE_PATH`   | Root S3 path for Iceberg warehouse (metadata + data files)             | `s3://acme-dev-datalake/iceberg/`                    |
| `--TABLE`            | Target Iceberg table name                                              | `corporate_registry`                                 |
| `--ICEBERG_LOCATION` | Physical S3 location of the specific Iceberg table (optional override) | `s3://acme-dev-datalake/iceberg/corporate_registry/` |


## ml_training_glue.py
### Required Job Parameters
The following parameters must be provided when running the Glue job.
| Parameter                     | Description                                               | Value                                                  |
| ----------------------------- | --------------------------------------------------------- | -------------------------------------------------------------- |
| `--GOLDEN_CORPORATE_REGISTRY` | S3 path to Golden-layer corporate registry dataset        | `s3://acme-dev-datalake/golden/corporate_registry/`            |
| `--OUTPUT_DATASET_PATH`       | S3 path for processed / feature-engineered dataset output | `s3://acme-dev-datalake/golden/ml/features/`                   |
| `--MODEL_OUTPUT_PATH`         | S3 path to store trained model artifacts                  | `s3://acme-dev-datalake/golden/ml/models/corporate_registry/`  |
| `--MODEL_REPORT_PATH`         | S3 path to store model evaluation reports and metrics     | `s3://acme-dev-datalake/golden/ml/reports/corporate_registry/` |

### Required Spark Configuration (Glue 5.0)

For all Iceberg jobs, the following Spark parameters must be configured in the AWS Glue job settings:

--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

--datalake-formats iceberg

Iceberg Scripts
- iceberg-create.py
- iceberg-upsert.py
- ml_training_glue.py
