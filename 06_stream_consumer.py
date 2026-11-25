
import os
import json
import time
from datetime import datetime

BASE_DIR = os.path.expanduser("~/member8_cdc_streaming")

INPUT_DIR = os.path.join(BASE_DIR, "output", "incoming")
PROCESSED_DIR = os.path.join(BASE_DIR, "output", "processed")
REJECTED_DIR = os.path.join(BASE_DIR, "output", "rejected")
DUPLICATES_DIR = os.path.join(BASE_DIR, "output", "duplicates")
LOG_DIR = os.path.join(BASE_DIR, "report")

ACCEPTED_FILE = os.path.join(
    PROCESSED_DIR, "accepted_events.jsonl"
)
REJECTED_FILE = os.path.join(
    REJECTED_DIR, "rejected_events.jsonl"
)
DUPLICATES_FILE = os.path.join(
    DUPLICATES_DIR, "duplicate_events.jsonl"
)
LOG_FILE = os.path.join(LOG_DIR, "streaming_run_log.txt")

REQUIRED_FIELDS = [
    "event_id",
    "operation",
    "event_time",
    "entity",
    "record_id"
]

VALID_OPERATIONS = ["INSERT", "UPDATE", "DELETE"]


def create_directories():
    directories = [
        PROCESSED_DIR,
        REJECTED_DIR,
        DUPLICATES_DIR,
        LOG_DIR
    ]

    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)


def validate_event(event):
    if not isinstance(event, dict):
        return False, "Event is not a JSON object", None

    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            return False, "Missing required field: " + field, None

    if event["operation"] not in VALID_OPERATIONS:
        return False, "Invalid operation type", None

    try:
        event_datetime = datetime.strptime(
            event["event_time"],
            "%Y-%m-%dT%H:%M:%SZ"
        )
    except (ValueError, TypeError):
        return False, "Invalid event timestamp", None

    return True, "Valid event", event_datetime


def write_json_line(file_handle, data):
    file_handle.write(json.dumps(data) + "\n")


def main():
    create_directories()

    output_files = [
        ACCEPTED_FILE,
        REJECTED_FILE,
        DUPLICATES_FILE,
        LOG_FILE
    ]

    for output_file in output_files:
        if os.path.exists(output_file):
            print("ERROR: Previous output already exists:")
            print(output_file)
            print("Do not overwrite an earlier run.")
            print("Use a fresh output location for another scenario.")
            return

    if not os.path.exists(INPUT_DIR):
        print("ERROR: Incoming directory not found.")
        return

    batch_files = sorted([
        name for name in os.listdir(INPUT_DIR)
        if name.endswith(".jsonl")
    ])

    if not batch_files:
        print("ERROR: No incoming batch files found.")
        return

    seen_event_ids = set()
    last_event_time = None

    accepted_count = 0
    rejected_count = 0
    duplicate_count = 0
    total_count = 0

    start_time = time.time()

    print("CDC CONSUMER STARTED")
    print("Incoming batches:", len(batch_files))
    print("----------------------------------")

    with open(ACCEPTED_FILE, "w") as accepted_file, \
         open(REJECTED_FILE, "w") as rejected_file, \
         open(DUPLICATES_FILE, "w") as duplicate_file, \
         open(LOG_FILE, "w") as log_file:

        for batch_name in batch_files:
            batch_path = os.path.join(INPUT_DIR, batch_name)
            batch_count = 0

            print("Processing:", batch_name)

            with open(batch_path, "r") as batch_file:
                for line_number, line in enumerate(batch_file, 1):
                    if not line.strip():
                        continue

                    total_count += 1
                    batch_count += 1

                    try:
                        event = json.loads(line)
                    except ValueError:
                        rejected_count += 1
                        write_json_line(rejected_file, {
                            "batch": batch_name,
                            "line": line_number,
                            "reason": "Invalid JSON",
                            "raw_event": line.strip()
                        })
                        continue

                    valid, reason, event_time = validate_event(event)

                    if not valid:
                        rejected_count += 1
                        write_json_line(rejected_file, {
                            "batch": batch_name,
                            "line": line_number,
                            "reason": reason,
                            "event": event
                        })
                        continue

                    event_id = event["event_id"]

                    if event_id in seen_event_ids:
                        duplicate_count += 1
                        write_json_line(duplicate_file, {
                            "batch": batch_name,
                            "reason": "Duplicate event_id",
                            "event": event
                        })
                        continue

                    if (
                        last_event_time is not None
                        and event_time < last_event_time
                    ):
                        rejected_count += 1
                        write_json_line(rejected_file, {
                            "batch": batch_name,
                            "reason": "Out-of-order event_time",
                            "event": event
                        })
                        continue

                    write_json_line(accepted_file, event)
                    accepted_count += 1
                    seen_event_ids.add(event_id)
                    last_event_time = event_time

            print("  Events read:", batch_count)

        end_time = time.time()
        duration = end_time - start_time

        average_time_ms = 0.0
        if total_count > 0:
            average_time_ms = (
                duration * 1000.0 / total_count
            )

        summary = [
            "",
            "CDC CONSUMER RUN SUMMARY",
            "------------------------",
            "Batches processed: " + str(len(batch_files)),
            "Total events read: " + str(total_count),
            "Accepted events: " + str(accepted_count),
            "Rejected events: " + str(rejected_count),
            "Duplicate events: " + str(duplicate_count),
            "Processing duration seconds: " + str(round(duration, 4)),
            "Average processing time ms/event: " + str(round(average_time_ms, 4))
        ]

        for item in summary:
            print(item)
            log_file.write(item + "\n")

    print("----------------------------------")
    print("CONSUMER FINISHED")


if __name__ == "__main__":
    main()
