# ContextQ - Data & AI Engineering & CI/CD Pipeline
### Table of Contents

- [Data Ingestion & Harmonization](#data-ingestion--harmonization)
  - [Data Sources](#data-sources)
  - [Data Architecture & Infrastructure](#data-architecture--infrastructure)
  - [Data Standardization](#data-standardization)
  - [Entity Resolution & Deduplication](#entity-resolution--deduplication)
- [Iceberg Upsert & Table Management](#iceberg-upsert--table-management)
- [ML Pipeline Implementation](#ml-pipeline-implementation)
  - [Feature Engineering](#feature-engineering)
  - [Model Training](#model-training)
  - [Model Tracking & Registration](#model-tracking--registration)
- [AWS Glue Configuration](#aws-glue-job-parameters--configuration-guide)
- [CI/CD and Testing](#cicd-and-testing)
  - [Continuous Integration](#continuous-integration-ci)
  - [Unit Tests](#unit-tests)
  - [Data Contract Check](#data-contract-check)
- [Bonus](#bonus)
  - [Adverse Media Processing](#adverse-media-news)
  - [Advanced Entity Resolution](#entity-resolution)

## Data Ingestion & Harmonization
### Data Sources
#### Source 1: Supply Chain / Company Profile Data
- **Dataset**: Top 1000 Global Tech Companies (2024)
- **Source**: Kaggle
- **Description**: Provides rankings and key business information of leading technology companies.
- **Columns**: `ranking`, `company`, `market_cap`, `stock`, `country`, `sector`, `industry`
- **Records**: 1,000
- **File**: [`technology_companies_raw.parquet`](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/dataset/technology_companies_raw.parquet)


#### Source 2: Financial & Company Metadata
- **Dataset**: 7+ Million Companies Dataset
- **Description**: Contains global company profiles, employee estimates, and industry information.
- **Columns**: `name`, `domain`, `year_founded`, `industry`, `size_range`, `locality`, `country`, `linkedin_url`, `current_employee_estimate`, `total_employee_estimate`
- **Records**: 7,173,426
- **File**: `companies_raw.parquet`
- **Sample File**: [`sample_companies_raw.parquet`](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/dataset/sample_companies_raw.parquet)

### Data Architecture & Infrastructure

The pipeline is built on an AWS S3-based data lake (`acme-dev-datalake`) with Apache Iceberg used for the final curated dataset.

#### Storage & Processing Layer
- **Data Lake**: Amazon S3 (`acme-dev-datalake`)
- **Bronze Layer**: Raw data stored as S3 objects (`acme-dev-datalake/bronze/`)
- **Silver Layer**: Processed and enriched data stored as S3 objects (`acme-dev-datalake/silver/`)
- **Gold Layer**: Aggregated and curated datasets stored as S3 objects (`acme-dev-datalake/golden/`)
- **Iceberg**: Upserted and incremental datasets derived from the Gold layer, stored as Apache Iceberg table (`acme-dev-datalake/iceberg/corporate_registry/`)
- **Architecture Pattern**: Medallion Architecture (Bronze → Silver → Gold -> Iceberg)

##### Bronze Layer (Raw Data)
Stores unprocessed source files in S3 object storage:
- `companies_raw.parquet`
- `technology_companies_raw.parquet`

##### Silver Layer (Processed & Enriched Data)
Contains cleaned, standardized, and transformed datasets stored as S3 objects:
- `tech_companies` (from `technology_companies_raw`)
- `company_registry` (from `companies_raw`)
- `fuzzy_match_output` (entity matching results generated using normalized company names and Levenshtein distance–based fuzzy matching)

##### Gold Layer (Harmonized Dataset)
Stores the final curated dataset to put in Apache Iceberg:
- Entity resolution performed using GraphFrames
- Unified corporate records with resolved `corporate_id`

##### Iceberg (Dataset)
- Supports ACID transactions and incremental upserts

##### Data Lake Directory Structure

```text
acme-dev-datalake/
├── bronze/                         # Raw source data (S3 objects)
│   ├── companies_raw.parquet
│   └── technology_companies_raw.parquet
│
├── silver/                         # Cleaned & processed data (S3 objects)
│   ├── tech_companies/
│   │   └── part-*.parquet
│   ├── company_registry/
│   │   └── part-*.parquet
│   └── fuzzy_match_output/
│       └── part-*.parquet
│
├── golden/                         # Curated & aggregated data (S3 objects)
│   └── corporate_registry/
│       └── part-*.parquet
│
└── iceberg/                        # Apache Iceberg tables (Upsert layer)
    └── corporate_registry/
        ├── metadata/
        ├── data/
        └── snapshots/
```

### Data Standardization

Both source datasets (`companies_df`  (from `companies_raw`) and `tech_df` (from `technology_companies_raw`) ) undergo standardization using [silver_layer_transformation_glue.py](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/aws_glue_jobs/silver_layer_transformation_glue.py) before matching.

#### Normalization
- **Lowercasing & Trimming**: All column values are lowercased and trimmed of extra whitespace
- **Canonical IDs**: Unique `record_id` generated using hash-based IDs computed from:
  - `company`
  - `country`
  - `industry`
  - `source_system`
    
- **Company Names**: Normalized via `normalize_company_name_str()`
  - Legal suffixes removed (e.g., Inc., Ltd., GmbH)
  - Punctuation and extra spaces stripped
  - Consistent formatting applied
    
- **Market Cap**: Converted to USD using current exchange rates
- **Estimated Revenue**: Derived as `market_cap_usd / 8` for ML feature engineering
  
#### Standardization
- **Industry**: Standardized via `standardize_industry()`
- **Country**: Standardized via `standardize_country()`

### Entity Resolution / Deduplication

#### Blocking & Candidate Generation
To minimize computational complexity, a blocking strategy reduces the search space:

##### Blocking Key
- Generated using the **first three characters** of normalized company names
- Prevents full Cartesian joins during matching phase
- Significantly reduces candidate pair comparisons

#### Fuzzy Matching
Candidate pairs identified through blocking undergo fuzzy matching:
##### Matching Algorithm
- **Levenshtein Distance**: Computed between normalized company names
- **Threshold Filtering**: Early filtering applied for performance optimization
- **Scalable Processing**: Batched processing handles 7M+ records efficiently

#### Graph-Based Entity Resolution

**GraphFrames** is used to assign a unique `corporate_id` across datasets using [`graphframe_corporate_id_glue.py`](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/aws_glue_jobs/graphframe_corporate_id_glue.py)

| Component     | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| **Vertices**  | Unique `record_id` values from both source datasets (`companies_df` and `tech_df`) |
| **Edges**     | Fuzzy-matched record pairs from previous matching phase                      |
| **Algorithm** | Connected Components analysis to group related records into clusters         |
| **Canonical ID** | Minimum `record_id` in each component is selected as the `corporate_id`   |

The resolved `corporate_id` is attached back to source datasets.
And the final curated dataset is generated by joining  **`tech_companies`** and **`company_registry`** datasets on `corporate_id`

### Iceberg Upsert & Table Management

#### Iceberg Table Setup
Setup steps:
- Grant edit permissions to the AWS Glue role in Lake Formation
- Create the database: `acme_datalake`
- Initialize the Iceberg table using [iceberg-create.py](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/aws_glue_jobs/iceberg-create.py)

#### Upsert (MERGE INTO) Operation
The Gold layer dataset is merged into `acme_datalake.corporate_registry` using [iceberg-upsert.py](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/aws_glue_jobs/iceberg-upsert.py)
Upsert logic:
- Matches records using `corporate_id`
- Updates existing records with enriched attributes (e.g current_employee_estimate)
- Inserts new corporate entities
- Ensures ACID-compliant, transactional updates
- **Sample File**: [`iceberg-table-sample.csv`](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/iceberg-table-sample.csv)
- **Athena View**
 <img width="1189" height="231" alt="Blank diagram (9)" src="https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/AthenaIcbergTable.png" />
  


## ML Pipeline Implementation

### Training Data Source
Data is loaded from the Gold-layer Iceberg table: `acme_datalake.corporate_registry`

### Feature Engineering
Following features are derived using the engineer_features function from [ml_training_glue.py](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/aws_glue_jobs/ml_training_glue.py)

| Feature                           | Description                              |
| --------------------------------- | ---------------------------------------- |
| `industry_ohe`                    | One-Hot Encoded Industry  representation of the industry_std|
| `company_age`                     | Years since company founding             |
| `log_company_age`                 | Log-transformed company age              |
| `log_reg_total_employee_estimate` | Log-normalized employee size             |
| `log_tech_estimated_revenue`      | Log-normalized revenue (target variable) |

### Feature Vector Assembly
Engineered features are combined using VectorAssembler:
| Output Column | Input Features                                                       |
| ------------- | -------------------------------------------------------------------- |
| `features`    | `log_reg_total_employee_estimate`, `log_company_age`, `industry_ohe` |

### Model Training
Linear Regression model is trained to predict normalized company revenue:
| Parameter         | Value                        | Description         |
| ----------------- | ---------------------------- | ------------------- |
| `labelCol`        | `log_tech_estimated_revenue` | Target variable     |
| `featuresCol`     | `features`                   | Input features      |
| `maxIter`         | 200                          | Training iterations |
| `regParam`        | 0.1                          | Regularization      |
| `elasticNetParam` | 0.5                          | L1/L2 balance       |

The dataset is split for model evaluation: **80% Training, 20% Testing**

### Model Tracking & Registration
- Training outputs are persisted for reproducibility
- Saved model artifacts as S3 objects 
- Stored feature datasets as S3 objects 
- Logged metrics (MAE, train/test size) as S3 objects.  [Sample Model Report](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/mlmodelreports/20260126.json)
- These artifacts enable lightweight model versioning and performance monitoring.


## AWS Glue Job Parameters & Configuration Guide: [Link](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/aws_glue_jobs/README.md)

## CI/CD and Testing

### AWS Glue ETL CI/CD & Orchestration Architecture [Bonus: Orchestration & Scheduling]
The entire workflow is orchestrated using AWS Step Functions.
<img width="800" height="550" alt="AWS Glue ETL CI CD and Orchestration Architecture" src="https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/AWS%20Glue%20ETL%20CI%20CD%20and%20Orchestration%20Architecture.png" />


#### Architecture Overview
The solution is built using the following AWS services:

- GitHub: Source code repository
- AWS CodeBuild: CI/CD build and deployment
- Amazon S3: Storage for scripts and data layers
- AWS Step Functions: Workflow orchestration
- AWS Glue: ETL processing
- Apache Iceberg on S3: Analytical data lake storage
- AWS Secret Manager: Secure storage for credentials and configuration for Amazon S3 and AWS Step Functions.

#### High-Level Flow

1. Developers push code changes to GitHub.
2. AWS CodeBuild is triggered automatically.
3. CodeBuild accesses secrets, runs tests and deploys Glue scripts to Amazon S3.
4. CodeBuild then triggers AWS Step Functions.
5. Step Functions orchestrates multiple AWS Glue jobs.
6. Glue jobs process data and write outputs to S3 and Iceberg tables.

### Continuous Integration (CI)
When code is pushed to the repository, AWS CodeBuild installs dependencies, runs pytest unit tests, and validates the build environment before deployment. 
If all tests pass, Glue scripts are synced to Amazon S3 and the Step Functions workflow is triggered automatically using [buildspec.yml](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/buildspec.yml)

#### Unit Tests
[test_normalize_company_name.py](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/tests/test_normalize_company_name.py) uses pytest to `validate the normalize_company_name_str()` function with multiple input cases to ensure correct company name normalization.

#### Data Contract Check
[iceberg-upsert.py](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/aws_glue_jobs/iceberg-upsert.py) uses the `validate_schema()` function to compare staging data with the target Iceberg table schema before merge/upsert, preventing incompatible data updates.



## Bonus:
### Adverse media news
- Running LLM-based adverse media extraction within the Glue PySpark upsert pipeline increases cost and latency. Since this enrichment is not required in real time and the assignment does not explicitly mandate real-time processing, it can be decoupled from the core merge process.
- We can move LLM processing to an asynchronous, serverless workflow using AWS Lambda. News ingestion and entity resolution can run independently throughout the day, store results in Amazon S3, and be batch-merged into the Iceberg table once per day.

#### Adverse media news Architecture
##### Serverless Adverse Media Processing Pipeline
<img width="800" height="550" alt="Blank diagram (8)" src="https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/Serverless%20Adverse%20Media%20Processing%20Pipeline.png" />

##### High-Level Flow
1. Company metadata files (containing `corporate_id` and `company_name`) are uploaded to Amazon S3.
2. An S3 event triggers the dispatcher AWS Lambda function.
3. The dispatcher Lambda reads the JSON file row by row.
4. For each record, the dispatcher extracts `corporate_id` and `company_name`.
5. The dispatcher invokes the `get_adverse_news` Lambda with the extracted parameters and the current date.
6. The processing Lambda uses OpenAI LLM to retrieve and analyze adverse news.
7. The enriched results are stored in the destination S3 bucket for downstream merging and analytics.

###### AWS Lambda Function Implementation: [get_adverse_news](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/tree/master/get_adverse_news)

##### Adverse Media Iceberg Upsert Pipeline
<img width="1189" height="231" alt="Blank diagram (9)" src="https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/Adverse%20Media%20Iceberg%20Upsert%20Pipeline.png" />

##### High-Level Flow
1. AWS Glue is triggered on a scheduled basis or by an event.
2. Glue reads the processed files from S3.
3. Glue performs data validation, cleansing, and deduplication.
4. Cleaned and deduplicated records are upserted into the Iceberg table.

### Entity Resolution
- Instead of running LLM inference directly inside Spark (which is inefficient and costly at scale), we can using the LLM for entity resolution only on ambiguous records, rather than across the entire dataset. Specifically, LLM-based resolution is applied to instances where the fuzzy matching score is greater than or equal to 0.60 and less than 0.80.
- For these cases, only essential attributes such as the company name from both datasets, along with industry and country information are passed to the LLM in batch mode. The model is then prompted to determine whether the two records refer to the same corporation.

<img width="1178" height="306" alt="Blank diagram (10)" src="https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/blob/master/Batch%20Entity%20Resolution.png" />

###### AWS Lambda Function Implementation: [batch_entity_resolution](https://github.com/Pahulpreet86/ContextQ-Data-AI-Engineering-CI-CD-Pipeline/tree/master/batch_entity_resolution)

