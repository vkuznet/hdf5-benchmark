#!/usr/bin/env python3
"""
h5_parallel_bench.py

MPI-parallel benchmark: each rank reads a subset of datasets from the given HDF5 file using the MPI driver.
Collects per-rank CPU/RAM/IO samples (via psutil) and reduces summaries to rank 0.

Usage:
    mpirun -n 4 python h5_parallel_bench.py --file big_test.h5 --out parallel_results.json

Dependencies:
    pip install mpi4py h5py numpy psutil
    (NOTE: h5py must be built with MPI support to use driver='mpio')
"""
import argparse
import time
import os
import json
import numpy as np
import psutil
from mpi4py import MPI
import h5py
import threading

def parse_args():
    import sys
    p = argparse.ArgumentParser()
    p.add_argument("--file", "-f", required=True, help="HDF5 file to read")
    p.add_argument("--out", default="parallel_results.json", help="Output JSON file (rank0 will write)")
    p.add_argument("--poll-interval", type=float, default=0.2, help="Sampling interval (s)")
    p.add_argument("--sample-reads", type=int, default=50, help="Random reads per rank")
    p.add_argument("--method", default="mpar", help="Method for h4py to use mpar or swmr")
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

def read_assigned(h5f, assigned_ds, sample_reads):
    # assigned_ds: list of dataset paths this rank should read
    # Read entire assigned datasets sequentially
    total_read = 0
    t0 = time.time()
    for name in assigned_ds:
        d = h5f[name]
        arr = d[...]
        total_read += arr.nbytes
        _ = arr.sum(dtype=np.float64)
    t_full = time.time() - t0
    # random partial reads (sample_reads)
    total_rand = 0
    t0 = time.time()
    for _ in range(sample_reads):
        if not assigned_ds:
            break
        p = np.random.choice(assigned_ds)
        d = h5f[p]
        shape = d.shape
        if len(shape) == 1:
            arr = d[:min(1024, shape[0])]
        else:
            arr = d[:min(4, shape[0]), :min(1024, shape[1])]
        total_rand += arr.nbytes
        _ = arr.mean()
    t_rand = time.time() - t0
    return {"full_bytes": int(total_read), "full_time": t_full, "rand_bytes": int(total_rand), "rand_time": t_rand}

def main():
    args = parse_args()
    comm = MPI.COMM_WORLD
    print(f"MPI rank={comm.rank} size={comm.size}")
    rank = comm.rank
    size = comm.size

    # start stats sampler for this rank
    sampler = StatsSampler(pid=os.getpid(), interval=args.poll_interval)
    sampler.start()

    if args.method == "mpar":
        # open file collectively
        try:
            h5f = h5py.File(args.file, "r", driver="mpio", comm=comm)
        except Exception as e:
            if rank == 0:
                print("ERROR opening HDF5 with MPI driver:", e)
                print("Ensure h5py was built with MPI support. Falling back to serial open (may not be parallel).")
            # fallback to serial file open
            h5f = h5py.File(args.file, "r")
    else:
        h5f = h5py.File(args.file, "r", swmr=True)

    # get list of datasets (let rank 0 gather and scatter)
    if rank == 0:
        ds_paths = list_datasets(h5f)
    else:
        ds_paths = None
    ds_paths = comm.bcast(ds_paths, root=0)

    # distribute dataset paths round-robin
    assigned = [p for i,p in enumerate(ds_paths) if (i % size) == rank]
    # perform read workload
    result = read_assigned(h5f, assigned, args.sample_reads)

    # stop sampler and collect samples
    sampler.stop()
    sampler.join()
    # reduce summaries to root
    local_summary = {
        "rank": rank,
        "n_assigned": len(assigned),
        "assigned_bytes_estimate": sum(h5f[p].size * h5f[p].dtype.itemsize for p in assigned),
        "read_full_bytes": result["full_bytes"],
        "read_full_time": result["full_time"],
        "read_rand_bytes": result["rand_bytes"],
        "read_rand_time": result["rand_time"],
    }
    # send local samples length and data to root (potentially large)
    # we'll compress samples into a small summary object per rank to avoid very large MPI messages
    if sampler.samples:
        first = sampler.samples[0]
        last = sampler.samples[-1]
        proc_read_delta = last.get('proc_read_bytes', 0) - first.get('proc_read_bytes', 0)
        sys_read_delta = last.get('sys_read_bytes', 0) - first.get('sys_read_bytes', 0)
    else:
        proc_read_delta = 0
        sys_read_delta = 0
    local_stats = {
        "proc_read_delta": int(proc_read_delta),
        "sys_read_delta": int(sys_read_delta),
        "duration": sampler.samples[-1]['time'] - sampler.samples[0]['time'] if sampler.samples else 0,
        "cpu_samples": [s["cpu_percent"] for s in sampler.samples] if sampler.samples else [],
        "mem_max": max((s.get("mem_rss",0) for s in sampler.samples), default=0),
    }

    all_summaries = comm.gather(local_summary, root=0)
    all_stats = comm.gather(local_stats, root=0)

    if rank == 0:
        out = {"file": args.file, "ranks": size, "summaries": all_summaries, "stats": all_stats}
        with open(args.out, "w") as fo:
            json.dump(out, fo, indent=2)
        print(f"{args.method} benchmark results written to {args.out}")

    h5f.close()

if __name__ == "__main__":
    main()

