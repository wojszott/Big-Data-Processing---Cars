#!/usr/bin/env python3
import sys

current_key = None
total_rentals = 0
completed_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        key, values = line.split('\t')
        rentals, completed = map(int, values.split(','))
    except ValueError:
        continue

    if key == current_key:
        total_rentals += rentals
        completed_count += completed
    else:
        if current_key:
            completed_ratio = completed_count / total_rentals if total_rentals > 0 else 0
            print(f"{current_key}\t{total_rentals},{completed_ratio:.2f}")
        current_key = key
        total_rentals = rentals
        completed_count = completed

if current_key:
    completed_ratio = completed_count / total_rentals if total_rentals > 0 else 0
    print(f"{current_key}\t{total_rentals},{completed_ratio:.2f}")
