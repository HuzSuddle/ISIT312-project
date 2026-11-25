# Enat Bank – Big Data Data Lake Project

**Course:** ISIT312 – Big Data Management  
**Project:** Enat Bank Data Lake Architecture and Implementation  
**Institution:** University of Wollongong in Dubai

## Overview

This project explores the design and implementation of a Big Data Data Lake architecture for **Enat Bank S.C.**, a private commercial bank based in Ethiopia.

The project examines the bank's existing data infrastructure, identifies challenges associated with data management and analytics, and proposes an improved architecture using a Data Lake approach.

A working prototype was developed using synthetic banking datasets to demonstrate data ingestion, storage, processing, transformation, quality validation and analytical querying using Hadoop ecosystem technologies.

The solution organises banking data into three layers:

- **Raw:** Stores the original source datasets.
- **Cleaned:** Contains validated and transformed datasets.
- **Curated:** Stores processed and aggregated data prepared for reporting and analytics.

## Objectives

- Design a scalable Data Lake architecture for banking data.
- Demonstrate data ingestion and distributed storage using Hadoop HDFS.
- Implement data cleaning, validation and transformation.
- Organise data into Raw, Cleaned and Curated layers.
- Enable SQL-based analytics through Apache Hive.
- Perform analytical processing using Apache Spark.
- Simulate incremental transaction processing using Python micro-batches.
- Explore customer-level data access through an optional HBase extension.
- Evaluate query correctness and processing performance across different dataset sizes.

## Architecture

The proposed architecture follows a layered Data Lake approach.

**Data Sources → Ingestion → HDFS Data Lake → Processing → Analytics and Reporting**

### Data Lake Layers

| Layer | Description |
|---|---|
| Raw | Original customer, account and transaction CSV datasets |
| Cleaned | Validated and transformed datasets |
| Curated | Aggregated analytical outputs, regulatory reports and customer summaries |
| Quality | Rejected records, validation results and row-count reconciliation reports |

### Processing and Analytics

- **Python:** Data cleaning, validation, transformation and CDC simulation.
- **Hadoop HDFS:** Distributed storage for the Data Lake.
- **YARN / MapReduce:** Execution framework for Hadoop-based batch processing.
- **Apache Hive:** External tables and SQL-based analytical queries.
- **Apache Spark:** Distributed data processing and Spark SQL analytics.
- **HBase:** Proposed customer-level lookup extension, with a customer-summary export prepared for integration.

### Proposed vs. Implemented Components

The proposed enterprise architecture includes Kafka and Spark Structured Streaming for real-time event processing. However, the prototype implements incremental processing through a Python-based file micro-batch simulation.

| Component | Implementation Status |
|---|---|
| Hadoop HDFS | Implemented and tested |
| Python batch processing | Implemented and tested |
| Hive SQL | Implemented and tested |
| Spark SQL | Implemented and tested |
| Python CDC micro-batch simulation | Implemented and tested |
| Kafka | Proposed |
| Spark Structured Streaming | Proposed |
| HBase | Optional extension; customer-summary export verified |

## Technologies Used

| Technology | Purpose |
|---|---|
| Apache Hadoop 2.7.3 | Distributed storage and processing |
| HDFS | Data Lake storage |
| YARN / MapReduce | Batch job execution |
| Apache Hive 2.1.1 | SQL querying and data analysis |
| Apache Spark 3.0.3 | Analytical processing |
| Python 3 | Data preparation and CDC simulation |
| HBase | Proposed NoSQL customer lookup extension |
| Ubuntu Linux | Development and testing environment |
| Oracle VirtualBox | Virtual machine environment |

## Dataset

The prototype uses synthetic banking data rather than Enat Bank's confidential production data.

The baseline datasets contain:

| Dataset | Records |
|---|---:|
| Customers | 200 |
| Accounts | 300 |
| Transactions | 2,000 |

The analytical pipeline also includes larger synthetic datasets for performance testing.

| Dataset Size | Customers | Accounts | Transactions |
|---|---:|---:|---:|
| Small | 200 | 300 | 2,000 |
| Large | 2,000 | 3,000 | 20,000 |
| Extra Large | 20,000 | 30,000 | 200,000 |

## Key Features

### 1. Data Ingestion and Storage
- Ingests synthetic banking datasets into Hadoop HDFS.
- Organises data into Raw, Cleaned and Curated zones.
- Maintains separate directories for data quality reports and rejected records.

### 2. Data Cleaning and Validation
- Processes customer, account and transaction records using Python.
- Validates and transforms incoming datasets.
- Generates row-count reconciliation reports.
- Separates rejected records for quality monitoring.

### 3. Analytical Processing
Three analytical jobs are provided:

- Daily transaction summaries and reconciliation.
- Customer and account activity analysis.
- Exception and high-value transaction analysis.

### 4. Hive and Spark Analytics
- Creates Hive external tables over the cleaned and curated HDFS datasets.
- Executes SQL-based analytical queries.
- Runs equivalent analytical jobs using Spark SQL.
- Verifies query outputs against independently calculated reference results.

### 5. Incremental Data Processing Simulation
A Python-based CDC (Change Data Capture) simulation demonstrates incremental transaction processing.

- Reads synthetic transaction events in JSONL format.
- Processes events in micro-batches of five.
- Handles INSERT and UPDATE operations.
- Identifies duplicate events.
- Rejects out-of-order events.
- Supports testing with continuously arriving event files.

### 6. Performance Evaluation
- Tests analytical jobs using small, large and extra-large datasets.
- Records execution timings across multiple trials.
- Compares Hive and Spark analytical results.
- Verifies outputs against expected reference datasets.

## Repository Structure

```
ISIT312-project/
│
├── Member5.zip
├── Member6_Submission (2).zip
├── Member7_Submission.zip
├── Member9_Submission.zip
├── README.md
└── environment_matrix_and_runbook.md
```

## Execution Environment

The prototype was tested using an Ubuntu Linux virtual machine running through Oracle VirtualBox.

The documented testing configuration includes:

- 2 virtual CPUs
- Approximately 8 GB available guest memory
- Java 8
- Hadoop 2.7.3
- Hive 2.1.1
- Spark 3.0.3
- Python 3

## Running the Project

The following commands provide an overview of the batch processing and analytics workflow. For the complete setup instructions, refer to [`environment_matrix_and_runbook.md`](environment_matrix_and_runbook.md).

### 1. Start Hadoop Services

```bash
start-dfs.sh
start-yarn.sh
jps
```

### 2. Run Data Cleaning and Curation

```bash
cd ~/member6_batch_pipeline

python3 code/02_clean_curate.py
bash code/01_hdfs_load.sh
```

### 3. Start Hive Metastore and Create Tables

```bash
nohup /usr/share/hive/bin/hive --service metastore \
  > output/hive-metastore.log 2>&1 &
```

```bash
hive -f code/03_hive_ddl.hql
hive -f code/04_validation_queries.hql
```

### 4. Execute Hive Analytical Jobs

```bash
cd ~/member7_jobs

bash code/05_run_jobs_timed.sh small 3

python3 code/07_verify_outputs.py \
  --engine hive --dataset small
```

### 5. Execute Spark Analytical Jobs

```bash
spark-submit code/spark/04_sample_jobs_spark.py \
  --master 'local[2]' \
  --lake hdfs://localhost:8020/bank_lake \
  --dataset small --trials 3
```

Verify the Spark results:

```bash
python3 code/07_verify_outputs.py \
  --engine spark --dataset small
```

### 6. Run Performance Tests

```bash
bash code/06_load_perf_datasets.sh large
bash code/05_run_jobs_timed.sh large 3
```

For the extra-large dataset:

```bash
bash code/06_load_perf_datasets.sh xlarge
bash code/05_run_jobs_timed.sh xlarge 2
```

## Testing and Results

The prototype demonstrated the following results:

- Successfully processed the baseline customer, account and transaction datasets.
- Generated 500 aggregated records in the curated daily regulatory report.
- Completed data validation with no rejected records in the baseline run.
- Successfully executed Hive analytical jobs across all three dataset sizes.
- Successfully executed Spark SQL analytical jobs on the small dataset.
- Matched analytical outputs against independently calculated reference results.
- Recorded 33 successful Hive and Spark analytical job runs.
- Processed 100 valid CDC events, including 68 INSERT and 32 UPDATE events.
- Detected duplicate events and rejected out-of-order events.
- Generated a customer-summary export containing 200 unique customer records for the proposed HBase extension.

These results demonstrate the functionality of the local prototype and do not represent production performance measurements or a live integration with Enat Bank systems.

## Limitations

- The prototype uses synthetic datasets rather than confidential production banking data.
- Testing was conducted in a resource-limited VirtualBox environment.
- Kafka and Spark Structured Streaming were proposed but not implemented.
- CDC processing was demonstrated through Python-based file micro-batches.
- Live integration with Enat Bank's existing banking systems was outside the prototype scope.
- HBase customer-summary export was verified, but actual HBase loading and lookup were not established.

## Future Improvements

- Integrate Kafka for continuous event ingestion.
- Implement Spark Structured Streaming for real-time processing.
- Establish HBase integration for customer-level queries.
- Expand data validation and monitoring capabilities.
- Integrate with existing enterprise data warehouse and business intelligence systems.
- Evaluate the architecture using larger datasets and production-like infrastructure.

## Documentation

For additional technical details, refer to:

| File | Description |
|---|---|
| `Member5.zip` | Additional project submission |
| `Member6_Submission (2).zip` | Batch ingestion, cleaning and Hive table implementation |
| `Member7_Submission.zip` | Analytical processing, Hive and Spark jobs, performance evaluation and verification |
| `Member9_Submission.zip` | Additional project submission |
| `environment_matrix_and_runbook.md` | Environment setup, execution instructions, testing procedures and results |
---

**Disclaimer:** This project was developed for academic purposes as part of ISIT312 – Big Data Management. It demonstrates a proposed Data Lake architecture through a local prototype and is not a production banking system.
