# Environment Matrix and Runbook

ISIT312 Big Data Management  
Enat Bank — Data Lake Prototype

## 1. Testing environment

The prototype runs on personal laptops using Ubuntu Linux virtual machines in Oracle VirtualBox. The configuration below was recorded during the analytical tests on 23 November 2026. It describes the tested VM rather than the specifications of every laptop used in the project.

| Environment | Configuration |
|---|---|
| Virtualization | Oracle VirtualBox |
| Operating system | Ubuntu Linux (x86_64), kernel 4.4.0-31-generic |
| Virtual CPU | 2 vCPUs |
| Available guest memory | 7,983 MB |
| Java | Java 8 (documented runtime requirement) |

Only synthetic banking data is used. The prototype does not connect to Enat Bank's production databases or reproduce its enterprise infrastructure.

### Software and services

| Software | Version / configuration | Use in the project | Status |
|---|---|---|---|
| Hadoop HDFS | 2.7.3 | Raw, Cleaned, Curated and Quality directories | Tested |
| YARN / MapReduce | Hadoop 2.7.3 | Execution of Hive batch jobs | Tested |
| Hive | 2.1.1; MapReduce engine | External tables and analytical SQL queries | Tested |
| Hive Metastore | Hive 2.1.1; port 9083 | Table definitions and locations | Tested |
| Python | Python 3 | Cleaning, validation, initial curation and CDC simulation | Tested |
| Spark SQL | 3.0.3; `local[2]` | Analytical queries on cleaned data | Tested |
| Kafka | — | Future event transport | Proposed only |
| Spark Structured Streaming | — | Future incremental event processing | Proposed only |
| HBase | — | Customer-level lookups | Optional; customer-summary export verified |

The exact VirtualBox version, Ubuntu release, Java build, Python minor version and VM disk allocation were not recorded in the available evidence. Test status is based on the supplied execution logs and reports. HBase lookup execution is not established by the CSV export alone.

## 2. Running the batch pipeline

The instructions assume that the configured BigdataVM is available, with the required software already installed. Extract the batch and analytics packages into the VirtualBox shared folder (`/media/sf_vmshare`). The existing folder names are retained because the supplied scripts and source archives use them.

### 2.1 Prepare the workspace

```bash
SHARE=/media/sf_vmshare
BATCH_SRC="$SHARE/Member6_Submission"
ANALYTICS_SRC="$SHARE/Member7_Submission"
BATCH="$HOME/member6_batch_pipeline"
ANALYTICS="$HOME/member7_jobs"

mkdir -p "$BATCH"/{input,code,output,evidence,report}
cp "$BATCH_SRC"/03_Input_Data/*.csv "$BATCH/input/"
cp "$BATCH_SRC"/02_Source_Code/* "$BATCH/code/"

mkdir -p "$ANALYTICS/results"
cp -r "$ANALYTICS_SRC"/code "$ANALYTICS_SRC"/perf_data "$ANALYTICS/"
cp -r "$ANALYTICS_SRC"/results/expected_* "$ANALYTICS/results/"
```

Adjust the shared-folder location if necessary. Archive any existing outputs before repeating the tests, as some scripts overwrite data or append timing records.

### 2.2 Start Hadoop

```bash
export HADOOP_HOME=/usr/share/hadoop
export HIVE_HOME=/usr/share/hive
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin"

# Start services only if they are not already running.
start-dfs.sh
start-yarn.sh

jps
hdfs dfs -ls /
```

Check that the NameNode, DataNode, ResourceManager and NodeManager appear in the process list. The HDFS directory listing should run without a connection error.

To record the configuration of the VM being used:

```bash
lsb_release -a
uname -a
nproc
free -m
df -h
java -version
python3 --version
hadoop version
hive --version
spark-submit --version
```

### 2.3 Clean and load the data

The Python script validates and transforms the original CSV files locally, before the raw files and processed outputs are uploaded into their corresponding HDFS directories.

```bash
cd "$HOME/member6_batch_pipeline"
ls input/
python3 code/02_clean_curate.py
cat output/row_count_report.csv
bash code/01_hdfs_load.sh
hdfs dfs -ls -R /bank_lake/
```

The baseline run produced the following record counts, excluding headers:

| Dataset | Input | Accepted | Rejected |
|---|---:|---:|---:|
| Customers | 200 | 200 | 0 |
| Accounts | 300 | 300 | 0 |
| Transactions | 2,000 | 2,000 | 0 |

The curated daily regulatory report contains 500 aggregated records. Rejected records and row-count reconciliation are stored separately in the Quality directory. No records were rejected in this baseline run.

The HDFS layout after batch processing and the subsequent customer-summary export is:

```text
/bank_lake/
  raw/
    customers.csv
    accounts.csv
    transactions.csv
  cleaned/
    customers/customers_clean.csv
    accounts/accounts_clean.csv
    transactions/transactions_clean.csv
  curated/
    regulatory_daily/bank_regulatory_daily.csv
    customer_summary/
    hbase_feed/customer_summary_for_hbase.csv
  quality/
    row_count_report.csv
    rejects/
      customers_rejects.csv
      accounts_rejects.csv
      transactions_rejects.csv
```

The CDC simulation uses separate JSONL files and does not rely on this HDFS batch loader.

### 2.4 Create and check the Hive tables

```bash
cd "$HOME/member6_batch_pipeline"

# Start the Metastore only if it is not running.
nohup /usr/share/hive/bin/hive --service metastore \
  > output/hive-metastore.log 2>&1 &

# Allow the service to initialize before checking it.
jps
netstat -lnt | grep ':9083'

hive -f code/03_hive_ddl.hql
hive -f code/04_validation_queries.hql
```

The Metastore should be listening on port 9083. If `netstat` is unavailable, use the equivalent socket-listing command in the VM. If startup fails, inspect `output/hive-metastore.log`.

Open the Hive CLI using `hive` and run the following checks:

```sql
USE bank_analytics;
SHOW TABLES;
SELECT COUNT(*) FROM bank_customers;
SELECT COUNT(*) FROM bank_accounts;
SELECT COUNT(*) FROM bank_transactions;
SELECT COUNT(*) FROM bank_regulatory_daily;
DESCRIBE FORMATTED bank_transactions;
SET hive.execution.engine;
```

| Table | HDFS location | Expected rows |
|---|---|---:|
| `bank_customers` | `/bank_lake/cleaned/customers` | 200 |
| `bank_accounts` | `/bank_lake/cleaned/accounts` | 300 |
| `bank_transactions` | `/bank_lake/cleaned/transactions` | 2,000 |
| `bank_regulatory_daily` | `/bank_lake/curated/regulatory_daily` | 500 |

The tables are external, comma-delimited TEXTFILE tables with one skipped header line. The supplied definitions use fixed columns; date partitioning and automatic schema evolution were not demonstrated by this DDL.

## 3. Analytical processing

Three analytical jobs are provided: daily transaction summaries and reconciliation, customer/account activity, and exception and high-value transaction review. Complete the batch pipeline before running them.

### 3.1 Hive SQL

```bash
cd "$HOME/member7_jobs"
bash code/05_run_jobs_timed.sh small 3
python3 code/07_verify_outputs.py --engine hive --dataset small
```

The verification script compares the query results with independently calculated reference outputs. The recorded result was `ALL OUTPUTS MATCH`.

### 3.2 Performance tests with larger datasets

The larger synthetic datasets are stored separately under `/bank_lake_perf/` to avoid replacing the baseline data.

| Dataset | Customers | Accounts | Transactions | Hive database |
|---|---:|---:|---:|---|
| Small | 200 | 300 | 2,000 | `bank_analytics` |
| Large | 2,000 | 3,000 | 20,000 | `bank_analytics_large` |
| Extra-large | 20,000 | 30,000 | 200,000 | `bank_analytics_xlarge` |

```bash
bash code/06_load_perf_datasets.sh large
bash code/05_run_jobs_timed.sh large 3
python3 code/07_verify_outputs.py --engine hive --dataset large

bash code/06_load_perf_datasets.sh xlarge
bash code/05_run_jobs_timed.sh xlarge 2
python3 code/07_verify_outputs.py --engine hive --dataset xlarge
```

The recorded Hive results matched the reference outputs at all three dataset sizes. Two trials were recorded for the extra-large dataset.

### 3.3 Spark SQL

Spark was tested on the small dataset in local mode. The default implementation reads cleaned HDFS CSV files and registers SQL views; it does not require a direct connection to the Hive Metastore.

```bash
hdfs getconf -confKey fs.defaultFS

spark-submit code/spark/04_sample_jobs_spark.py \
  --master 'local[2]' \
  --lake hdfs://localhost:8020/bank_lake --dataset small --trials 3

python3 code/07_verify_outputs.py --engine spark --dataset small
python3 code/09_summarise_timings.py
```

Use the NameNode URI reported by `hdfs getconf` if it differs from the example. The recorded Spark results also matched the reference outputs. The original timing log contains 33 successful Hive and Spark job runs. These measurements describe the local prototype and cannot be used to predict the bank's production reporting times.

## 4. Additional demonstrations

### 4.1 Python CDC simulation

The CDC implementation uses a file-based producer and consumer to process synthetic transaction changes in micro-batches. It was developed separately from the core HDFS batch pipeline. Kafka and Spark Structured Streaming remain part of the proposed architecture, rather than the implemented prototype.

| Script | Function |
|---|---|
| `05_emit_cdc_events.py` | Reads `cdc_events.jsonl` and emits batches of five events. |
| `06_stream_consumer.py` | Processes batches, validates events and separates accepted, rejected and duplicate records. |
| `06_stream_consumer_scenarios.py` | Tests duplicate and out-of-order events. |
| `07_live_cdc_producer.py` | Emits new batches while the consumer runs. |
| `08_live_stream_consumer.py` | Monitors the input directory and processes newly arrived files. |

For the live-arrival test, start the consumer in one Ubuntu terminal before starting the producer in another. Use the CDC implementation's own input/output paths and invocation instructions. The report identifies the scripts and execution order, but does not specify all arguments or directory settings.

The recorded tests processed 100 valid events (68 INSERT and 32 UPDATE), detected a duplicate and rejected an out-of-order event. The live-arrival test also processed all 100 events. These results demonstrate file-based incremental processing, not a deployed Kafka/Spark streaming pipeline. No HDFS connection for the CDC outputs is established by the available evidence.

### 4.2 Customer-summary export and HBase

The customer summary created by the analytical jobs is exported for the optional HBase lookup extension.

```bash
cd "$HOME/member7_jobs"
bash code/08_export_hbase_feed.sh
hdfs dfs -ls /bank_lake/curated/hbase_feed/
python3 code/07_verify_outputs.py --hbase-csv results/customer_summary_for_hbase_vm.csv
```

The export contains 200 unique customer records and is available in HDFS at `/bank_lake/curated/hbase_feed/customer_summary_for_hbase.csv`. It is generated from `bank_analytics.bank_customer_summary`.

The proposed HBase table is `bank_customer_summary`, using `customer_id` as the row key and `profile` and `metrics` as proposed column families. The CSV export was verified; HBase loading and lookup require separate implementation evidence.

## 5. Integration and verification

The interfaces below describe how the implemented stages exchange data. The optional CDC simulation is kept separate from the batch pipeline.

| Source | Destination | Interface | Processing mode |
|---|---|---|---|
| Synthetic datasets | HDFS Raw | Original CSV files | Batch |
| Synthetic datasets | Python cleaning and curation | CSV validation and transformation | Batch |
| Python outputs | HDFS Cleaned, Curated and Quality | Cleaned CSVs, aggregates and quality reports | Batch |
| HDFS datasets | Hive | External table definitions over HDFS files | Batch / SQL |
| Hive tables and cleaned HDFS data | Hive and Spark SQL jobs | SQL and Spark views | Batch |
| Hive customer summary | HBase export | Customer-keyed CSV | Batch export |
| Synthetic CDC dataset | Python producer and consumer | JSONL micro-batches | Incremental simulation |
| Future CDC source | Kafka, Spark Structured Streaming and HDFS | Proposed event-processing interface | Streaming (proposed) |
| Curated datasets | Existing EDW and BI tools | Future approved connector or export | Proposed integration |

For integration verification, compare file schemas and identifiers, reconcile input and output counts, check Hive table locations and compare query results with the reference outputs. The batch and analytical evidence establishes the interfaces through the customer-summary export. It does not establish a complete end-to-end test including HBase or live bank connectivity.

### Execution records

Keep the environment logs, HDFS listings, reconciliation files, Hive query outputs, analytical verification reports, timing logs and CDC test outputs with the project code. Preserve the original `results/vm_run/` directory as the record of completed analytical tests.

Run repeat tests only in the local prototype environment. The batch loading script overwrites HDFS files, and the customer-summary job rewrites its curated output. Do not format the NameNode to resolve an ordinary startup problem, and do not stop shared services while other tests are running.

## 6. Supporting files

- Batch pipeline package: `Member6_Submission.zip` — setup README, source scripts, Hive definitions and processed datasets.
- Analytical jobs package: `Member7_Submission.zip` — execution scripts, VM runbook, reference outputs, performance datasets and completed verification logs under `results/vm_run/`.
- Project report: CDC Implementation and Technical Testing and Results of the CDC Implementation — producer/consumer workflow and test results.

This runbook consolidates the documented execution procedures and results. The combined procedure has not been independently rerun on a fresh virtual machine.
