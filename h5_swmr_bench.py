#!/usr/bin/env python3
"""
h5_swmr_bench.py

Read an HDF5 file produced by gen_h5_xxx.py and measure I/O/CPU/RAM stats while performing several access patterns:
 - Full scan of all datasets (sequential read)
 - Random sample reads
 - Metadata-only scan

Usage:
    python h5_swmr_bench.py --file big_test.h5 --runs 1 --sample-reads 100 --out swmr_results.json

Dependencies:
    pip install h5py numpy psutil
"""
import argparse
import time
import h5py
import numpy as np
from mpi4py import MPI
import psutil
import json
import os
import threading
from collections import defaultdict

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--file", "-f", required=True, help="HDF5 file to read")
    p.add_argument("--runs", type=int, default=1, help="How many times to repeat each access pattern")
    p.add_argument("--sample-reads", type=int, default=100, help="Number of random dataset reads for sampling")
    p.add_argument("--poll-interval", type=float, default=0.2, help="Sampling interval (s) for system stats")
    p.add_argument("--out", default="swmr_results.json", help="Output JSON for stats")
    return p.parse_args()

class StatsSampler(threading.Thread):
    def __init__(self, pid, interval=0.2):
        super().__init__()
        self.proc = psutil.Process(pid)
        self.interval = interval
        self.samples = []
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            t = time.time()
            try:
                cpu = self.proc.cpu_percent(interval=None)
                mem = self.proc.memory_info().rss
                io = self.proc.io_counters()
                sys_io = psutil.disk_io_counters()
                self.samples.append({
                    "time": t,
                    "cpu_percent": cpu,
                    "mem_rss": mem,
                    "proc_read_bytes": io.read_bytes,
                    "proc_write_bytes": io.write_bytes,
                    "sys_read_bytes": sys_io.read_bytes,
                    "sys_write_bytes": sys_io.write_bytes,
                })
            except Exception as e:
                # process might disappear
                self.samples.append({"time": t, "error": str(e)})
            time.sleep(self.interval)

    def stop(self):
        self.stop_event.set()

def list_datasets(h5f):
    ds_paths = []
    def visitor(name, obj):
        if isinstance(obj, h5py.Dataset):
            ds_paths.append(name)
    h5f.visititems(visitor)
    return ds_paths

def read_full_scan_orig(h5f):
    total_bytes = 0
    start = time.time()
    for name in list_datasets(h5f):
        d = h5f[name]
        # read entire dataset into memory (may be large)
        arr = d[...]
        total_bytes += arr.nbytes
        # short processing to avoid optimization-out
        _ = arr.sum(dtype=np.float64)
    elapsed = time.time() - start
    return {"bytes": int(total_bytes), "elapsed": elapsed}

def read_full_scan(h5file):
    """
    Read all numeric datasets in the HDF5 file and compute a checksum.
    Returns stats: elapsed time, throughput, memory, cpu usage.
    """
    proc = psutil.Process(os.getpid())
    start_mem = proc.memory_info().rss
    start_time = time.time()
    total_bytes = 0
    total_sum = 0.0

    def scan_group(grp):
        nonlocal total_bytes, total_sum
        for name, item in grp.items():
            if isinstance(item, h5py.Dataset):
                if np.issubdtype(item.dtype, np.number):
                    arr = item[()]
                    total_sum += arr.sum(dtype=np.float64)
                    total_bytes += arr.nbytes
                else:
                    # skip string/object datasets
                    continue
            elif isinstance(item, h5py.Group):
                scan_group(item)

    scan_group(h5file)

    elapsed = time.time() - start_time
    end_mem = proc.memory_info().rss
    cpu_percent = proc.cpu_percent(interval=None)
    throughput = total_bytes / (elapsed * 1024 * 1024)

    return {
        "elapsed": elapsed,
        "cpu_percent": cpu_percent,
        "mem_used_mb": (end_mem - start_mem) / 1024 / 1024,
        "throughput_mb_s": throughput,
        "bytes_read": total_bytes,
        "checksum": total_sum,
    }

def read_random_samples(h5f, n):
    # collect dataset names and their sizes
    ds_paths = list_datasets(h5f)
    sizes = [(p, h5f[p].size * h5f[p].dtype.itemsize) for p in ds_paths]
    # choose datasets randomly weighted by size
    total = sum(s for _, s in sizes) or 1
    probs = [s/total for _, s in sizes]
    chosen = np.random.choice([p for p,_ in sizes], size=min(n, len(ds_paths)), replace=True, p=probs)
    total_bytes = 0
    start = time.time()
    for p in chosen:
        d = h5f[p]
        # read a small slice (like 1% or up to 1MB) to simulate partial read
        shape = d.shape
        if len(shape) == 1:
            start_idx = 0
            count = max(1, min(shape[0], shape[0]//100))
            arr = d[:count]
        else:
            # read a block of rows
            rows = shape[0]
            read_rows = max(1, min(rows, rows//100))
            arr = d[:read_rows, :min(shape[1], 1024)]
        total_bytes += arr.nbytes
        _ = arr.mean()
    elapsed = time.time() - start
    return {"bytes": int(total_bytes), "elapsed": elapsed}

def metadata_scan(h5f):
    start = time.time()
    names = []
    for name in h5f:
        grp = h5f[name]
        # list attributes and keys
        _ = dict(grp.attrs)
        for sub in grp:
            _ = dict(grp[sub].attrs) if hasattr(grp[sub], 'attrs') else {}
        names.append(name)
    elapsed = time.time() - start
    return {"items": len(names), "elapsed": elapsed}

def main():
    args = parse_args()
    p = psutil.Process(os.getpid())
    sampler = StatsSampler(pid=os.getpid(), interval=args.poll_interval)
    sampler.start()

    results = {"file": args.file, "runs": args.runs, "samples": []}
    with h5py.File(args.file, "r", swmr=True) as f:
        # Warm-up read of metadata
        #f.refresh()
        # perform patterns
        for r in range(args.runs):
            res = {}
            t0 = time.time()
            res['metadata_scan'] = metadata_scan(f)
            # full scan may be large — measure carefully
            res['full_scan'] = read_full_scan(f)
            res['random_samples'] = read_random_samples(f, args.sample_reads)
            res['total_elapsed'] = time.time() - t0
            results['samples'].append(res)

    sampler.stop()
    sampler.join()
    results['sys_samples'] = sampler.samples
    # compute throughput and summary
    # compute proc read bytes delta
    if sampler.samples:
        first = sampler.samples[0]
        last = sampler.samples[-1]
        proc_read_delta = last.get('proc_read_bytes', 0) - first.get('proc_read_bytes', 0)
        sys_read_delta = last.get('sys_read_bytes', 0) - first.get('sys_read_bytes', 0)
        results['summary'] = {
            'proc_read_delta': int(proc_read_delta),
            'sys_read_delta': int(sys_read_delta),
            'duration': sampler.samples[-1]['time'] - sampler.samples[0]['time'],
        }
    else:
        results['summary'] = {}

    with open(args.out, "w") as fo:
        json.dump(results, fo, indent=2)
    print("Results saved to", args.out)

if __name__ == "__main__":
    main()

