
import os
import json
import copy

BASE_DIR = os.path.expanduser("~/member8_cdc_streaming")
INPUT_DIR = os.path.join(BASE_DIR, "output", "scenario_input")


def read_first_event():
    path = os.path.join(INPUT_DIR, "batch_0001.jsonl")

    with open(path, "r") as f:
        first_line = f.readline()

    return json.loads(first_line)


def write_batch(filename, event):
    path = os.path.join(INPUT_DIR, filename)

    with open(path, "w") as f:
        f.write(json.dumps(event) + "\n")


def main():
    original = read_first_event()

    # Test 1: Repeat an existing event with the same event_id.
    duplicate = copy.deepcopy(original)

    # Test 2: Create a unique event with an earlier timestamp.
    out_of_order = copy.deepcopy(original)
    out_of_order["event_id"] = "TEST_OUT_OF_ORDER_001"
    out_of_order["event_time"] = "2026-11-01T05:59:00Z"

    write_batch("batch_0021.jsonl", duplicate)
    write_batch("batch_0022.jsonl", out_of_order)

    print("TEST EVENTS CREATED")
    print("Duplicate event ID:", duplicate["event_id"])
    print("Out-of-order event ID:", out_of_order["event_id"])
    print("Out-of-order timestamp:", out_of_order["event_time"])
    print("Scenario batch files: 22")


if __name__ == "__main__":
    main()
