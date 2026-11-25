# Enat Bank – Big Data Data Lake Project

**Course:** ISIT312 – Big Data Management
**Project:** Enat Bank Data Lake Architecture and Implementation
**Institution:** University of Wollongong in Dubai

> Items marked **[CONFIRM]** could not be verified from the repository zip (it has no Member 8 submission, no diagram files and no interest-reporting results). Check each one with its owner, then delete the marker.

## Overview

This project explores the design and implementation of a Big Data Data Lake architecture for **Enat Bank S.C.**, a private commercial bank based in Ethiopia.

The project examines the bank's existing data infrastructure, identifies challenges with data management and analytics, and proposes an improved architecture using a Data Lake approach.

The proposed on-premise data lake **complements the bank's existing enterprise data warehouse (EDW)** rather than replacing it. It reduces reliance on complex reporting queries run against the Oracle physical standby database, which the bank reported as the source of hour-long regulatory reports.

A working prototype was built with synthetic banking datasets to demonstrate ingestion, storage, processing, transformation, quality validation and analytical querying with Hadoop ecosystem technologies.

The solution organises banking data into three storage zones:

- **Raw:** the original source datasets.
- **Cleaned:** validated and transformed datasets.
- **Curated:** processed and aggregated data prepared for reporting and analytics.

A separate **Quality** directory holds rejected records, validation results and row-count reconciliation reports. It supports the three zones and is **not** a fourth storage zone.

## Objectives

- Design a scalable on-premise Data Lake architecture for banking data.
- Demonstrate ingestion and distributed storage using Hadoop HDFS.
- Implement data cleaning, validation and transformation.
- Organise data into Raw, Cleaned and Curated zones.
- Enable SQL-based analytics through Apache Hive.
- Run analytical processing with Apache Spark SQL.
- Simulate incremental transaction processing with Python micro-batches.
- Explore customer-level data access through an optional HBase serving layer.
- Evaluate query correctness and processing performance across dataset sizes.

## Architecture

Two diagrams describe the solution:

| Diagram | File | Purpose |
|---|---|---|
| Target (logical) architecture | `docs/target_architecture.png` **[CONFIRM path]** | Data flow from sources to analytics, including proposed components |
| Deployment and scope | `docs/deployment_and_scope_diagram.png` **[CONFIRM path]** | Which components were demonstrated in the lab and which are proposed only |

**Data Sources → Ingestion → HDFS Data Lake → Processing → Analytics and Reporting**

### Pipelines

| Pipeline | Description | Status |
|---|---|---|
| Batch pipeline | CSV files → HDFS raw → Python cleaning → cleaned/curated → Hive external tables → Hive and Spark SQL jobs | Implemented and tested |
| Python CDC pipeline | Simulated JSONL change events → Python micro-batch consumer → separate CDC output paths (does not overwrite batch tables) | Implemented and tested (simulation) |
| HBase serving layer (optional) | Customer-summary export keyed by `customer_id` (column families `profile`, `metrics`) loaded into standalone HBase for point lookups | Implemented and tested (standalone mode, small dataset) |

### Data lake zones

| Location | Description |
|---|---|
| Raw | Original customer, account and transaction CSV datasets |
| Cleaned | Validated and transformed datasets |
| Curated | Aggregated analytical outputs, regulatory reports and customer summaries |
| Quality (supporting directory) | Rejected records, validation results and row-count reconciliation reports |

### Processing and analytics components

- **Python:** data cleaning, validation, transformation and CDC simulation.
- **Hadoop HDFS:** distributed storage for the data lake.
- **YARN:** cluster resource management (schedules and allocates resources).
- **MapReduce:** the batch processing engine that runs on YARN (used by Hive jobs on Hadoop 2.7.3).
- **Apache Hive:** external tables and SQL analytical queries.
- **Apache Spark:** Spark SQL analytics.
- **HBase:** optional customer-level lookup layer.

### Proposed vs. implemented components

The proposed enterprise architecture includes Kafka and Spark Structured Streaming. The prototype implements incremental processing with a Python file micro-batch simulation.

| Component | Status |
|---|---|
| Hadoop HDFS | Implemented and tested |
| Python batch processing | Implemented and tested |
| Hive SQL | Implemented and tested |
| Spark SQL (local mode) | Implemented and tested |
| Python CDC micro-batch simulation | Implemented and tested |
| Kafka | Proposed only |
| Spark Structured Streaming | Proposed only |
| HBase (standalone, small dataset) | Implemented and tested; 200 rows loaded, verifier 12 of 12 passed |
| Production security controls (access control, masking, encryption at rest) | Proposed only |

## Technologies Used

| Technology | Purpose |
|---|---|
| Apache Hadoop 2.7.3 | Distributed storage and processing |
| HDFS | Data lake storage |
| YARN | Resource management |
| MapReduce | Batch job execution engine |
| Apache Hive 2.1.1 | SQL querying and analysis |
| Apache Spark 3.0.3 | Spark SQL analytical processing (local mode) |
| Python 3 | Data preparation and CDC simulation |
| HBase | Optional NoSQL customer lookup layer |
| Ubuntu Linux | Development and testing environment |
| Oracle VirtualBox | Virtual machine environment |

## Dataset

The prototype uses synthetic banking data, not Enat Bank's confidential production data. The data is generated by `Member5/code/generate_synthetic_data.py` (seed 312).

| Dataset Size | Customers | Accounts | Transactions | CDC events |
|---|---:|---:|---:|---:|
| Small (default) | 200 | 300 | 2,000 | 100 |
| Large | 2,000 | 3,000 | 20,000 | 1,000 |
| Extra Large | 20,000 | 30,000 | 200,000 | 100 (unused) |

All three sets of counts were checked against the generator manifests in `Member7_Submission/perf_data/`. The extra-large set was generated with explicit counts (`--customers 20000 --accounts 30000 --transactions 200000`), not a preset.

## Repository Structure

Listed as in the repository zip. **Member 8's CDC submission and the architecture diagram files are not in the repository**, so add them before submission.

```
ISIT312-project/
│
├── README.md
├── environment_matrix_and_runbook.md
├── member5.zip                       # synthetic data, generator, validation, data dictionary, ERD
├── Member6_Submission (2).zip        # batch ingestion, cleaning, Hive tables
├── Member7_Submission.zip            # Hive/Spark jobs, timings, verification
├── Member8_Submission.zip            # CDC simulation  [NOT IN REPOSITORY - add it; name taken from Member 9's defect log]
├── Member9_Submission.zip            # HBase lookup, integration QA
└── docs/                             # architecture diagrams  [NOT IN REPOSITORY - add them]
```

## Execution Environment

Tested on an Ubuntu Linux virtual machine in Oracle VirtualBox:

- 2 virtual CPUs
- Approximately 8 GB guest memory
- Java 8, Hadoop 2.7.3, Hive 2.1.1, Spark 3.0.3 (local mode), Python 3

## Running the Project

For full setup, see [`environment_matrix_and_runbook.md`](environment_matrix_and_runbook.md). Commands were cross-checked against the member READMEs and the runbook. They were not re-run on the VM.

### 1. Start Hadoop services

```bash
start-dfs.sh
start-yarn.sh
jps
```

### 2. Generate synthetic data (optional; data is already included)

```bash
cd Member5
python3 code/generate_synthetic_data.py
python3 code/validate_data.py --fixtures
```

### 3. Run data cleaning and curation

```bash
cd ~/member6_batch_pipeline
python3 code/02_clean_curate.py
bash code/01_hdfs_load.sh
```

Cleaning runs first and the HDFS load second, as in Member 6's README. Scripts are in `Member6_Submission/02_Source_Code/`.

### 4. Start the Hive metastore and create tables

```bash
nohup /usr/share/hive/bin/hive --service metastore \
  > output/hive-metastore.log 2>&1 &

hive -f code/03_hive_ddl.hql
hive -f code/04_validation_queries.hql
```

### 5. Run the Hive analytical jobs

```bash
cd ~/member7_jobs
bash code/05_run_jobs_timed.sh small 3
python3 code/07_verify_outputs.py --engine hive --dataset small
```

### 6. Run the Spark SQL jobs (local mode)

```bash
spark-submit code/spark/04_sample_jobs_spark.py \
  --master 'local[2]' \
  --lake hdfs://localhost:8020/bank_lake \
  --dataset small --trials 3

python3 code/07_verify_outputs.py --engine spark --dataset small
```

`--master` is an argument of the job script itself, so it goes after the script path, as in the runbook. Check the port with `hdfs getconf -confKey fs.defaultFS` (the runbook does the same).

### 7. Run the CDC simulation

Script names come from the runbook (section 4.1). Member 8's submission is not in the repository, so its arguments and input/output paths are **[CONFIRM against Member 8's run card]**.

| Script | Function |
|---|---|
| `05_emit_cdc_events.py` | Emits `cdc_events.jsonl` in micro-batches of five |
| `06_stream_consumer.py` | Validates events; separates accepted, rejected and duplicate records |
| `06_stream_consumer_scenarios.py` | Duplicate and out-of-order test scenarios |
| `07_live_cdc_producer.py` and `08_live_stream_consumer.py` | Live-arrival test: start the consumer in one terminal, then the producer in another |

```bash
python3 05_emit_cdc_events.py      # add Member 8's documented options
python3 06_stream_consumer.py      # add Member 8's documented options
```

Member 9's QA check notes that the CDC outputs are kept apart from the batch tables. Confirm the actual output paths with Member 8.

### 8. Load and query HBase (optional)

From Member 9's package (`bash code/07a_hbase_load.sh` first, then the lookup and verifier):

```bash
bash code/07a_hbase_load.sh
hbase shell -n code/07_hbase_lookup.txt
python3 code/07b_verify_hbase.py       # add --scan/--get/--csv inputs as in Member 9's README
bash code/07d_qa_checks.sh
```

### 9. Run the performance tests

```bash
bash code/06_load_perf_datasets.sh large
bash code/05_run_jobs_timed.sh large 3

bash code/06_load_perf_datasets.sh xlarge
bash code/05_run_jobs_timed.sh xlarge 2
```

## Testing and Results

- Processed the baseline customer, account and transaction datasets.
- Generated 500 aggregated records in the curated daily regulatory report.
- Completed validation with no rejected records in the baseline run.
- Ran Hive analytical jobs on all three dataset sizes.
- Ran Spark SQL analytical jobs on the small dataset in local mode.
- Matched analytical outputs against independently calculated reference results.
- Recorded 33 successful Hive and Spark analytical job runs.
- Processed 100 valid CDC events (68 INSERT, 32 UPDATE); detected duplicate events and rejected out-of-order events.
- Generated a customer-summary export of 200 unique customer records for the HBase serving layer.
- HBase (standalone): table created with families `profile` and `metrics`, 200 customers loaded (2,932 non-empty cells), `GET C000001` matched the CSV, a missing key returned no row, and the verifier passed 12 of 12 checks.

### Supplementary interest-reporting comparison

A supplementary interest-reporting comparison measured **26.1 seconds versus 2.8 seconds** **[CONFIRM: which two approaches or engines, dataset size and number of trials; these figures are not in the repository]**. This is a **measurement on a synthetic prototype dataset in a resource-limited VirtualBox VM**. It is not a measurement of Enat Bank production systems and must not be read as evidence of a production speed-up or compared directly with the bank's reported ~1-hour report.

These results demonstrate the functionality of the local prototype. They do not represent production performance or a live integration with Enat Bank systems.

## Limitations

- The prototype uses synthetic data rather than confidential production banking data.
- Testing ran in a resource-limited VirtualBox environment.
- Spark SQL was tested only in local mode (`local[2]`), not on a multi-node cluster.
- Kafka and Spark Structured Streaming were proposed but not implemented.
- CDC was demonstrated through Python file micro-batches on simulated events. The bank reports no log-based CDC.
- Schema evolution and DELETE events were not demonstrated.
- Production security controls (role-based access, masking on export, encryption at rest) remain proposed and were not implemented in the prototype.
- Live integration with Enat Bank systems was outside the prototype scope.
- HBase ran in standalone mode on the small dataset only. Re-running the load to confirm repeatability was not completed (interrupted by a ZooKeeper outage), and the Hive-vs-HBase single-customer timing was not recorded in the QA checklist.

## Future Improvements

- Integrate Kafka for continuous event ingestion.
- Implement Spark Structured Streaming.
- Complete HBase integration for customer-level queries.
- Demonstrate schema evolution and DELETE-event handling.
- Implement encryption at rest, role-based access and masking.
- Integrate with the existing EDW and BI tools (Oracle BI, Metabase).
- Evaluate on larger datasets, on a multi-node cluster and on production-like infrastructure.

## Documentation

| File | Description |
|---|---|
| `member5.zip` | Synthetic data, generator, validation, data dictionary, ERD, lineage |
| `Member6_Submission (2).zip` | Batch ingestion, cleaning and Hive tables |
| `Member7_Submission.zip` | Hive and Spark jobs, performance evaluation, verification |
| `Member8_Submission.zip` | CDC simulation **[CONFIRM name]** |
| `Member9_Submission.zip` | HBase lookup and integration tests |
| `environment_matrix_and_runbook.md` | Environment setup, execution instructions, testing procedures |

---

**Disclaimer:** This project was developed for academic purposes as part of ISIT312 – Big Data Management. It demonstrates a proposed Data Lake architecture through a local prototype and is not a production banking system.
