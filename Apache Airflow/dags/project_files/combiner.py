#!/usr/bin/env python3
import sys

current_key = None
total = 0
completed = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        key, value = line.split("\t")
        rentals, comp = map(int, value.split(","))
    except ValueError:
        continue  # pomijamy błędne linie

    if key == current_key:
        total += rentals
        completed += comp
    else:
        if current_key is not None:
            print(f"{current_key}\t{total},{completed}")
        current_key = key
        total = rentals
        completed = comp

if current_key is not None:
    print(f"{current_key}\t{total},{completed}")
