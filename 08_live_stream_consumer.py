
import os
import json
import time
from datetime import datetime

BASE_DIR = os.path.expanduser("~/member8_cdc_streaming")

INPUT_DIR = os.path.join(BASE_DIR, "output", "live_incoming")
RESULT_DIR = os.path.join(BASE_DIR, "output", "live_results")

PROCESSED_DIR = os.path.join(RESULT_DIR, "processed")
REJECTED_DIR = os.path.join(RESULT_DIR, "rejected")
DUPLICATES_DIR = os.path.join(RESULT_DIR, "duplicates")

ACCEPTED_FILE = os.path.join(PROCESSED_DIR, "live_accepted.jsonl")
REJECTED_FILE = os.path.join(REJECTED_DIR, "live_rejected.jsonl")
DUPLICATES_FILE = os.path.join(DUPLICATES_DIR, "live_duplicates.jsonl")

LOG_FILE = os.path.join(
    BASE_DIR, "report", "live", "live_processing_log.txt"
)

REQUIRED_FIELDS = [
    "event_id", "operation", "event_time", "entity", "record_id"
]

VALID_OPERATIONS = ["INSERT", "UPDATE", "DELETE"]


def create_directories():
    for path in [
        PROCESSED_DIR, REJECTED_DIR, DUPLICATES_DIR,
        os.path.dirname(LOG_FILE)
    ]:
        if not os.path.exists(path):
            os.makedirs(path)


def validate_event(event):
    if not isinstance(event, dict):
        return False, "Event is not a JSON object", None

    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            return False, "Missing field: " + field, None

    if event["operation"] not in VALID_OPERATIONS:
        return False, "Invalid operation", None

    try:
        event_time = datetime.strptime(
            event["event_time"], "%Y-%m-%dT%H:%M:%SZ"
        )
    except (ValueError, TypeError):
        return False, "Invalid event timestamp", None

    return True, "Valid", event_time


def main():
    create_directories()

    for path in [ACCEPTED_FILE, REJECTED_FILE,
                 DUPLICATES_FILE, LOG_FILE]:
        if os.path.exists(path):
            print("ERROR: Previous live output exists:", path)
            print("Use a fresh output folder for another run.")
            return

    if not os.path.exists(INPUT_DIR):
        print("ERROR: Live incoming folder not found.")
        return

    seen_ids = set()
    last_event_time = None
    processed_batches = set()

    total_events = 0
    accepted = 0
    rejected = 0
    duplicates = 0
    latency_values = []

    start_time = time.time()

    print("LIVE CDC CONSUMER STARTED")
    print("Watching:", INPUT_DIR)
    print("Polling interval: 0.2 seconds")
    print("----------------------------------")

    with open(ACCEPTED_FILE, "w") as accepted_file, \
         open(REJECTED_FILE, "w") as rejected_file, \
         open(DUPLICATES_FILE, "w") as duplicate_file, \
         open(LOG_FILE, "w") as log_file:

        while True:
            batch_names = sorted([
                name for name in os.listdir(INPUT_DIR)
                if name.startswith("live_batch_")
                and name.endswith(".jsonl")
                and name not in processed_batches
            ])

            for batch_name in batch_names:
                batch_path = os.path.join(INPUT_DIR, batch_name)
                batch_start = time.time()
                batch_events = 0

                print("DETECTED:", batch_name)

                with open(batch_path, "r") as batch_file:
                    for line_number, line in enumerate(batch_file, 1):
                        if not line.strip():
                            continue

                        total_events += 1
                        batch_events += 1

                        try:
                            event = json.loads(line)
                        except ValueError:
                            rejected += 1
                            rejected_file.write(json.dumps({
                                "batch": batch_name,
                                "reason": "Invalid JSON",
                                "raw_event": line.strip()
                            }) + "\n")
                            continue

                        valid, reason, event_time = validate_event(event)

                        if not valid:
                            rejected += 1
                            rejected_file.write(json.dumps({
                                "batch": batch_name,
                                "reason": reason,
                                "event": event
                            }) + "\n")
                            continue

                        event_id = event["event_id"]

                        if event_id in seen_ids:
                            duplicates += 1
                            duplicate_file.write(json.dumps({
                                "batch": batch_name,
                                "reason": "Duplicate event_id",
                                "event": event
                            }) + "\n")
                            continue

                        if (
                            last_event_time is not None
                            and event_time < last_event_time
                        ):
                            rejected += 1
                            rejected_file.write(json.dumps({
                                "batch": batch_name,
                                "reason": "Out-of-order event_time",
                                "event": event
                            }) + "\n")
                            continue

                        emitted_at = event.get(
                            "_simulation_emitted_at"
                        )

                        if emitted_at:
                            try:
                                emitted_time = datetime.strptime(
                                    emitted_at,
                                    "%Y-%m-%dT%H:%M:%S.%fZ"
                                )
                                delay = (
                                    datetime.utcnow() - emitted_time
                                ).total_seconds() * 1000.0
                                latency_values.append(delay)
                            except ValueError:
                                pass

                        accepted_file.write(json.dumps(event) + "\n")
                        accepted += 1
                        seen_ids.add(event_id)
                        last_event_time = event_time

                batch_delay = (time.time() - batch_start) * 1000.0

                message = (
                    "PROCESSED {} | events={} | batch_processing_ms={:.3f}"
                ).format(batch_name, batch_events, batch_delay)

                print(message)
                log_file.write(message + "\n")
                log_file.flush()

                processed_batches.add(batch_name)

            done_marker = os.path.join(INPUT_DIR, "PRODUCER_DONE")

            if os.path.exists(done_marker):
                remaining = [
                    name for name in os.listdir(INPUT_DIR)
                    if name.startswith("live_batch_")
                    and name.endswith(".jsonl")
                    and name not in processed_batches
                ]

                if not remaining:
                    break

            time.sleep(0.2)

        duration = time.time() - start_time

        average_latency = 0.0
        maximum_latency = 0.0

        if latency_values:
            average_latency = sum(latency_values) / len(latency_values)
            maximum_latency = max(latency_values)

        summary = [
            "",
            "LIVE CDC PROCESSING SUMMARY",
            "----------------------------",
            "Batches processed: " + str(len(processed_batches)),
            "Total events read: " + str(total_events),
            "Accepted events: " + str(accepted),
            "Rejected events: " + str(rejected),
            "Duplicate events: " + str(duplicates),
            "Total consumer duration seconds: " + str(round(duration, 4)),
            "Average emission-to-processing delay ms: " +
                str(round(average_latency, 3)),
            "Maximum emission-to-processing delay ms: " +
                str(round(maximum_latency, 3))
        ]

        for item in summary:
            print(item)
            log_file.write(item + "\n")

    print("----------------------------------")
    print("LIVE CONSUMER FINISHED")


if __name__ == "__main__":
    main()
