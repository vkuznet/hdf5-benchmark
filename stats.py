#!/usr/bin/env python3
import json
import sys
import os
import argparse

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--swmr", required=True, help="swmr file")
    p.add_argument("--mpar", required=True, help="mpi parallel file")
    return p.parse_args()

def human(n):
    for u in ['B','KB','MB','GB','TB']:
        if abs(n) < 1024.0 or u == 'TB':
            return f"{n:3.1f}{u}"
        n /= 1024.0

def main():
    args = parse_args()
    if os.path.exists(args.swmr):
        s=json.load(open(args.swmr))
        print("SWMR summary:")
        print("  file:", s.get('file'))
        rank_summaries = False
        for r in s.get('summaries',[]):
            rank_summaries = True
            print(f"   rank {r['rank']}: assigned {r['n_assigned']} datasets, read_full_bytes={human(r['read_full_bytes'])}, full_time={r['read_full_time']:.3f}s")
        if not rank_summaries:
            print("  samples:", len(s.get('samples',[])))
            print("  summary:", s.get('summary',{}))
    if os.path.exists(args.mpar):
        p=json.load(open(args.mpar))
        print("")
        print("Parallel summary:")
        print("  file:", p.get('file'))
        for r in p.get('summaries',[]):
            print(f"   rank {r['rank']}: assigned {r['n_assigned']} datasets, read_full_bytes={human(r['read_full_bytes'])}, full_time={r['read_full_time']:.3f}s")

if __name__ == "__main__":
    main()
