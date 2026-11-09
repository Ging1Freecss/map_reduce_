#!/usr/bin/env python3
"""
MapReduce Inspector
Displays raw input data and decodes binary intermediate partition files.
"""

import os
import sys
import glob
import struct
from collections import defaultdict

def decode_record_file(filepath):
    records = []
    if not os.path.exists(filepath):
        return records
    with open(filepath, 'rb') as f:
        data = f.read()
    offset = 0
    while offset < len(data):
        if offset + 4 > len(data):
            break
        key_len = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        if offset + key_len > len(data):
            break
        key = data[offset:offset+key_len].decode('utf-8', errors='replace')
        offset += key_len

        if offset + 4 > len(data):
            break
        val_len = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        if offset + val_len > len(data):
            break
        val = data[offset:offset+val_len].decode('utf-8', errors='replace')
        offset += val_len

        records.append((key, val))
    return records

def print_banner(title):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)

def inspect_input(input_path):
    print_banner(f"1. Master Input File: {input_path}")
    if not os.path.exists(input_path):
        print(f"  [ERROR] File not found: {input_path}")
        return
    file_size = os.path.getsize(input_path)
    print(f"  Size: {file_size} bytes")
    print("  Content:")
    print("  " + "-" * 50)
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        for idx, line in enumerate(f, 1):
            print(f"  [{idx:2d}] {line.rstrip()}")
    print("  " + "-" * 50)

def inspect_intermediate(intermediate_dir):
    print_banner(f"2. Intermediate Map Output Files ({intermediate_dir})")
    files = sorted(glob.glob(os.path.join(intermediate_dir, "mr-*_*.data")))
    if not files:
        print(f"  No intermediate files found in {intermediate_dir}")
        return

    # Group by map task ID
    by_map_task = defaultdict(list)
    by_reducer = defaultdict(list)
    all_records = []

    for filepath in files:
        basename = os.path.basename(filepath)
        # format: mr-<map_id>_<reduce_id>.data
        name_part = basename.replace("mr-", "").replace(".data", "")
        if "_" in name_part:
            m_id, r_id = name_part.split("_", 1)
            records = decode_record_file(filepath)
            by_map_task[int(m_id)].append((int(r_id), filepath, records))
            by_reducer[int(r_id)].extend(records)
            all_records.extend(records)

    # Display grouped by Map Task
    print("\n  >> Grouped by Map Task:")
    for map_id in sorted(by_map_task.keys()):
        print(f"\n  [Map Task {map_id}]:")
        for r_id, path, records in sorted(by_map_task[map_id], key=lambda x: x[0]):
            size_b = os.path.getsize(path)
            rec_str = ", ".join(f"('{k}', {v})" for k, v in records) if records else "(empty)"
            print(f"    -> Partition {r_id} ({os.path.basename(path)}, {size_b} B, {len(records)} recs): {rec_str}")

    # Display grouped by Reducer Partition
    print("\n  >> Grouped by Reducer Partition (What each Reducer will consume):")
    for r_id in sorted(by_reducer.keys()):
        recs = by_reducer[r_id]
        print(f"\n  [Reducer Partition {r_id}] ({len(recs)} total records):")
        if recs:
            for k, v in recs:
                print(f"    - {k:15s} => {v}")
        else:
            print("    (no records assigned to this partition)")

    # Aggregated Word Count preview
    print_banner("3. Verification: Aggregate Word Frequencies (Map Stage Output)")
    key_counts = defaultdict(int)
    for k, v in all_records:
        try:
            key_counts[k] += int(v)
        except ValueError:
            key_counts[k] += 1

    for word in sorted(key_counts.keys()):
        print(f"  {word:15s} : {key_counts[word]}")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    input_file = sys.argv[1] if len(sys.argv) > 1 else "data/sample.txt"
    intermediate_dir = sys.argv[2] if len(sys.argv) > 2 else "/tmp/mr_worker_intermediate"
    inspect_input(input_file)
    inspect_intermediate(intermediate_dir)

