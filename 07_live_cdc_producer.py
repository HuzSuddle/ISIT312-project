
import os
import json
import time
import argparse
from datetime import datetime

BASE_DIR = os.path.expanduser("~/member8_cdc_streaming")
INPUT_FILE = os.path.join(BASE_DIR, "input", "cdc_events.jsonl")
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "live_incoming")


def timestamp_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    if args.batch_size < 1 or args.interval < 0:
        print("ERROR: Invalid batch size or interval.")
        return

    if not os.path.exists(INPUT_FILE):
        print("ERROR: Original CDC input file not found.")
        return

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    if os.listdir(OUTPUT_DIR):
        print("ERROR: Live incoming folder is not empty.")
        print("Use a fresh folder for a new live demonstration.")
        return

    events = []

    with open(INPUT_FILE, "r") as source:
        for line in source:
            if line.strip():
                events.append(json.loads(line))

    if not events:
        print("ERROR: No CDC events found.")
        return

    total_batches = 0

    print("LIVE CDC PRODUCER STARTED")
    print("Total events:", len(events))
    print("Batch size:", args.batch_size)
    print("Interval:", args.interval, "seconds")
    print("----------------------------------")

    for start in range(0, len(events), args.batch_size):
        batch = events[start:start + args.batch_size]
        total_batches += 1

        emitted_at = timestamp_now()

        filename = "live_batch_{:04d}.jsonl".format(total_batches)
        final_path = os.path.join(OUTPUT_DIR, filename)
        temp_path = final_path + ".tmp"

        with open(temp_path, "w") as output:
            for event in batch:
                event["_simulation_emitted_at"] = emitted_at
                output.write(json.dumps(event) + "\n")

        os.rename(temp_path, final_path)

        print(
            "EMITTED {} | {} events | {}".format(
                filename, len(batch), emitted_at
            )
        )

        if start + args.batch_size < len(events):
            time.sleep(args.interval)

    done_path = os.path.join(OUTPUT_DIR, "PRODUCER_DONE")

    with open(done_path, "w") as done_file:
        done_file.write(timestamp_now() + "\n")

    print("----------------------------------")
    print("LIVE PRODUCER FINISHED")
    print("Total events emitted:", len(events))
    print("Total batches emitted:", total_batches)


if __name__ == "__main__":
    main()
