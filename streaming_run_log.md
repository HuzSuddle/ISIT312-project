

\# Member 8 (8966825) – CDC Simulation and Streaming Demonstration

\## ISIT312 – Big Data Management



\*\*Implementation:\*\* Python-based file-driven micro-batch simulation  

\*\*Environment:\*\* Ubuntu VirtualBox  

\*\*Python version:\*\* 3.4.3  

\*\*Source dataset:\*\* Member 5 – `cdc\_events.jsonl`  

\*\*Implementation status:\*\* Simulated and tested



\---



\## 1. Objective



The objective of Member 8's work package was to demonstrate Change Data Capture (CDC) event generation, delivery, processing and validation through a streaming-oriented architecture.



The implementation was designed to demonstrate how changes to transaction records can be emitted as events, received in small batches, validated and processed into separate outputs.



Apache Kafka and Apache Spark Structured Streaming were not deployed because of the limitations of the available virtual machine environment. Instead, a Python-based file-driven micro-batch simulation was implemented.



This is an academic simulation of the streaming workflow, not a production Kafka or Spark deployment.



\## 2. Source Dataset and Event Contract



The source dataset was provided by Member 5 in JSON Lines format.



Source file:



`input/cdc\_events.jsonl`



Dataset inspection results:



| Attribute | Result |

|---|---:|

| Total events | 100 |

| INSERT events | 68 |

| UPDATE events | 32 |

| DELETE events | 0 |

| Unique event IDs | 100 |

| Duplicate event IDs | 0 |

| Chronological ordering | Confirmed |



The events contain the following fields:



\- `schema\_version`

\- `source`

\- `before`

\- `event\_id`

\- `event\_time`

\- `entity`

\- `record\_id`

\- `after`

\- `operation`



The dataset represents simulated CDC events for the `transactions` entity.



\### Event processing rules



The following rules were implemented:



1\. `event\_id` is used as the unique event identifier for duplicate detection.

2\. `operation` identifies the type of change: INSERT, UPDATE or DELETE.

3\. `event\_time` is used to assess chronological ordering.

4\. `record\_id` identifies the affected transaction record.

5\. Events missing required fields or containing invalid operations or timestamps are rejected.

6\. Repeated event IDs are identified and routed to the duplicate output.

7\. Events with timestamps earlier than the last accepted event are rejected by the strict ordering check.



\## 3. System Architecture and Implementation



The prototype consists of two main components.



\### 3.1 CDC Producer



File: `code/05\_emit\_cdc\_events.py`



The initial producer reads the original CDC dataset and emits events in configurable batches.



Configuration used:



\- Batch size: 5 events.

\- Emission interval: 1 second.

\- Total source events: 100.

\- Total generated batches: 20.



Each batch is written as a separate JSON Lines file into:



`output/incoming/`



The original Member 5 source dataset is not modified.



\### 3.2 CDC Consumer



File: `code/06\_stream\_consumer.py`



The initial consumer reads the generated batch files and applies:



\- JSON parsing.

\- Required-field validation.

\- Operation validation.

\- Timestamp validation.

\- Duplicate event detection.

\- Chronological ordering checks.



Results are separated into:



\- `output/processed/`

\- `output/rejected/`

\- `output/duplicates/`



The consumer also records processing duration and average processing time per event.



\### 3.3 Live-arrival Producer and Consumer



Files:



\- `code/07\_live\_cdc\_producer.py`

\- `code/08\_live\_stream\_consumer.py`



A separate live-arrival demonstration was implemented to show the consumer detecting and processing batches while the producer was still emitting them.



The live producer:



\- Emits five events per batch.

\- Waits one second between batches.

\- Adds a simulation emission timestamp to each event.

\- Writes completed batch files into `output/live\_incoming/`.



The live consumer:



\- Checks the incoming directory every 0.2 seconds.

\- Detects newly available batch files.

\- Validates and processes events.

\- Records emission-to-processing delay.

\- Stops after the producer completion marker is detected and all available batches are processed.



Live processing results are stored separately under `output/live\_results/`.



\## 4. Testing and Results



Four scenarios were implemented and tested.



\### 4.1 Scenario 1 – Normal Processing



The producer emitted the original 100 events in 20 batches of five events each.



The consumer successfully processed all batches.



| Metric | Result |

|---|---:|

| Batches processed | 20 |

| Events read | 100 |

| Accepted events | 100 |

| Rejected events | 0 |

| Duplicate events | 0 |

| Processing duration | 0.0375 seconds |

| Average processing time | 0.3751 ms/event |



\*\*Outcome:\*\* Successful. All original events passed validation and were accepted.



\### 4.2 Scenario 2 – Duplicate Event Detection



A duplicate test event was deliberately introduced by repeating the first original event, retaining its existing event ID.



The repeated event was placed in `batch\_0021.jsonl`.



The consumer identified the repeated ID:



`E000001`



It was routed to the duplicate output rather than being accepted again.



\### 4.3 Scenario 3 – Out-of-Order Event Rejection



A new test event was introduced with:



\- Event ID: `TEST\_OUT\_OF\_ORDER\_001`

\- Event timestamp: `2026-11-01T05:59:00Z`



It was placed in `batch\_0022.jsonl`, after the original chronological sequence.



The consumer identified that its timestamp was earlier than the last accepted event timestamp and rejected it.



\### Combined Scenario Results



| Metric | Result |

|---|---:|

| Batches processed | 22 |

| Total events read | 102 |

| Accepted events | 100 |

| Rejected events | 1 |

| Duplicate events | 1 |

| Processing duration | 0.1041 seconds |

| Average processing time | 1.0203 ms/event |



\*\*Outcome:\*\* Successful. The duplicate and out-of-order events were correctly identified and separated from accepted events.



\### 4.4 Scenario 4 – Live-Arrival Demonstration



A separate producer and consumer were executed simultaneously in two Ubuntu terminal windows.



The consumer was started first and waited for incoming batches. The producer was then started in the second terminal.



The consumer detected and processed incoming batches while the producer was still running.



| Metric | Result |

|---|---:|

| Batches emitted | 20 |

| Batches processed | 20 |

| Events emitted | 100 |

| Events read | 100 |

| Accepted events | 100 |

| Rejected events | 0 |

| Duplicate events | 0 |

| Average emission-to-processing delay | 105.504 ms |

| Maximum emission-to-processing delay | 201.689 ms |

| Total consumer duration | 136.7877 seconds |



\*\*Outcome:\*\* Successful. All 100 events were processed during the live-arrival demonstration.



The emission-to-processing delay was measured using timestamps generated by the producer and read by the consumer on the same virtual machine.



The total consumer duration includes waiting time before and during production and should not be interpreted as processing time alone.



\## 5. Output Organisation



The implementation uses separate directories for source data, code, incoming batches and processing results.



Main output locations:



| Directory | Purpose |

|---|---|

| `input/` | Original Member 5 CDC dataset |

| `code/` | Producer, consumer and helper scripts |

| `output/incoming/` | Initial producer batch files |

| `output/processed/` | Normal accepted events |

| `output/rejected/` | Normal rejected events |

| `output/duplicates/` | Normal duplicate events |

| `output/scenario\_input/` | Separate controlled test batches |

| `output/scenario\_results/` | Duplicate and out-of-order test results |

| `output/live\_incoming/` | Live producer batch files |

| `output/live\_results/` | Live consumer processing results |

| `report/` | Processing logs and documentation |

| `evidence/` | Screenshots of implementation and tests |



The Member 8 outputs are kept separate from Member 6's batch-processing outputs.



\## 6. Execution Instructions



Run commands from the Ubuntu terminal.



\### Initial producer



```bash

cd \~/member8\_cdc\_streaming/code

python3 05\_emit\_cdc\_events.py --batch-size 5 --interval 1

```



\### Initial consumer



```bash

python3 06\_stream\_consumer.py

```



\### Duplicate and out-of-order scenario consumer



```bash

python3 06\_stream\_consumer\_scenarios.py

```



\### Live-arrival demonstration



Start the consumer in the first terminal:



```bash

python3 08\_live\_stream\_consumer.py

```



Start the producer in a second terminal:



```bash

python3 07\_live\_cdc\_producer.py --batch-size 5 --interval 1

```



The live consumer detects and processes the incoming batches while production is ongoing.



\## 7. Limitations and Evaluation



The implementation demonstrates the logical workflow of a CDC streaming pipeline using Python micro-batch processing.



However, it does not implement the infrastructure or guarantees of a production distributed streaming platform.



Key limitations include:



1\. No Apache Kafka broker, topic or distributed event log was deployed.

2\. No Apache Spark Structured Streaming engine was deployed.

3\. File-based batch delivery does not provide Kafka's partitioning, replication or distributed consumer coordination.

4\. Processing is limited to one Python consumer and one virtual machine.

5\. The strict timestamp check rejects every event older than the last accepted timestamp. It does not implement watermarking or an allowed-lateness window.

6\. The implementation does not provide production-grade fault tolerance, checkpoint recovery or exactly-once processing guarantees.

7\. The live-arrival demonstration uses local files and a shared virtual machine clock rather than measuring network transmission latency.



\### Why batch-only processing is insufficient for low-latency requirements



Traditional batch processing collects data over a period and processes it as a scheduled or manually initiated job. This introduces a delay between data generation and the availability of processed results.



A streaming-oriented architecture processes newly arriving events more frequently, making it more suitable for use cases requiring timely updates, such as transaction monitoring and fraud detection.



\### Why MapReduce is not ideal for continuous streaming



MapReduce is designed primarily for batch-oriented distributed processing. Its job execution and intermediate processing stages introduce overhead that makes it unsuitable for continuous, low-latency event processing.



Kafka combined with Spark Structured Streaming or another dedicated streaming engine would be a more appropriate architecture for a production-scale implementation.



\## 8. Conclusion



The Member 8 CDC prototype successfully demonstrated event generation, micro-batch delivery, event validation, duplicate detection, chronological ordering checks and live-arrival processing.



The original 100 CDC events were successfully processed in the normal and live-arrival scenarios. Additional controlled tests demonstrated the rejection of a duplicate event and an out-of-order event.



The implementation is classified as \*\*simulated and tested\*\*. It demonstrates the core principles of a streaming pipeline while acknowledging the limitations of a local Python file-based implementation compared with production Kafka and Spark infrastructure.



