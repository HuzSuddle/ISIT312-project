python3 - <<'PY'
import json
from collections import Counter

path = "input/cdc_events.jsonl"

events = []

with open(path, "r") as f:
    for line in f:
        if line.strip():
            events.append(json.loads(line))

print("========== CDC DATASET INSPECTION ==========")

print("Total events:", len(events))

print("\nEvent fields:")
print(list(events[0].keys()))

print("\nOperation counts:")
operations = Counter(e.get("operation") for e in events)
for operation, count in operations.items():
    print(operation, ":", count)

event_ids = [e.get("event_id") for e in events]

print("\nUnique event IDs:", len(set(event_ids)))
print("Duplicate event IDs:", len(event_ids) - len(set(event_ids)))

timestamps = [e.get("event_time") for e in events]

print("\nFirst event timestamp:", timestamps[0])
print("Last event timestamp:", timestamps[-1])

print("\nChronological ordering:",
      timestamps == sorted(timestamps))

print("\nFirst event:")
print(json.dumps(events[0], indent=2))

print("\n============================================")
PY