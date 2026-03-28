#!/usr/bin/env python3
import sys
import csv

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("rental_id"):
        continue
    try:
        rental_id, car_id,customer_id, rental_start, rental_end, price, status = next(csv.reader([line]))
        year = rental_start.split("-")[0]
        completed_flag = 1 if status.strip().lower() == "completed" else 0
        print(f"{car_id},{year}\t1,{completed_flag}")
    except Exception:
        continue
