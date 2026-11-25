
import os
import json
import time
import argparse

BASE_DIR = os.path.expanduser("~/member8_cdc_streaming")
INPUT_FILE = os.path.join(BASE_DIR, "input", "cdc_events.jsonl")
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "incoming")


def load_events():
    events = []

    with open(INPUT_FILE, "r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if line.strip():
                event = json.loads(line)
                events.append(line.rstrip("\n"))

    return events


def main():
    parser = argparse.ArgumentParser(
        description="Emit CDC events in simulated micro-batches"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Number of events in each batch"
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between batches"
    )

    args = parser.parse_args()

    if args.batch_size < 1:
        print("ERROR: Batch size must be at least 1.")
        return

    if args.interval < 0:
        print("ERROR: Interval cannot be negative.")
        return

    if not os.path.exists(INPUT_FILE):
        print("ERROR: CDC input file not found:", INPUT_FILE)
        return

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    existing = [
        name for name in os.listdir(OUTPUT_DIR)
        if name.endswith(".jsonl") or name.endswith(".tmp")
    ]

    if existing:
        print("ERROR: Incoming folder is not empty.")
        print("Please clear the old batch files before starting a new run.")
        return

    events = load_events()

    if not events:
        print("ERROR: No events found in the input file.")
        return

    total_events = len(events)
    total_batches = 0

    print("CDC PRODUCER STARTED")
    print("Total source events:", total_events)
    print("Batch size:", args.batch_size)
    print("Interval:", args.interval, "seconds")
    print("----------------------------------")

    for start in range(0, total_events, args.batch_size):
        batch = events[start:start + args.batch_size]
        total_batches += 1

        filename = "batch_{:04d}.jsonl".format(total_batches)
        final_path = os.path.join(OUTPUT_DIR, filename)
        temp_path = final_path + ".tmp"

        with open(temp_path, "w", encoding="utf-8") as output:
            for event_line in batch:
                output.write(event_line + "\n")

        os.rename(temp_path, final_path)

        print(
            "Emitted {}: {} events ({} of {})".format(
                filename,
                len(batch),
                min(start + len(batch), total_events),
                total_events
            )
        )

        if start + args.batch_size < total_events:
            time.sleep(args.interval)

    print("----------------------------------")
    print("PRODUCER FINISHED")
    print("Total events emitted:", total_events)
    print("Total batches emitted:", total_batches)


if __name__ == "__main__":
    main()
