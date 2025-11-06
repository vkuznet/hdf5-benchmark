#!/usr/bin/env python3
"""
gen_h5_str.py

Create a large, complex HDF5 file for benchmarking.

Usage:
    python gen_h5_str.py --out big_test.h5 --size-gb 4 --branches 8 --subgroups 8 --datasets-per-subgroup 6 --seed 42

This will create a hierarchical structure:
  /branch_0/sub_0/ds_0 ... ds_N
  /branch_0/sub_1/...
  ...
Each dataset is chunked and has attributes. Dataset sizes are chosen to sum (approximately) to the requested total size.

Dependencies:
    pip install h5py numpy
"""
import argparse
import math
import numpy as np
import h5py
import os
import sys
from tqdm import tqdm

def human(n):
    for u in ['B','KB','MB','GB','TB']:
        if abs(n) < 1024.0 or u == 'TB':
            return f"{n:3.1f}{u}"
        n /= 1024.0

def parse_args():
    p = argparse.ArgumentParser(description="Generate large, complex HDF5 file for I/O benchmarking.")
    p.add_argument("--out", "-o", required=True, help="Output HDF5 file")
    p.add_argument("--size-gb", type=float, default=1.0, help="Approx target file size in GB")
    p.add_argument("--branches", type=int, default=4, help="Top-level branches (groups)")
    p.add_argument("--subgroups", type=int, default=8, help="Subgroups per branch")
    p.add_argument("--datasets-per-subgroup", type=int, default=4, help="Datasets per subgroup")
    p.add_argument("--min-ds-kb", type=int, default=64, help="Minimum dataset size in KB")
    p.add_argument("--max-ds-mb", type=int, default=64, help="Maximum dataset size in MB")
    p.add_argument("--seed", type=int, default=0, help="Random seed")
    p.add_argument("--chunks-kb", type=int, default=64, help="Target chunk size in KB")
    p.add_argument("--overwrite", action="store_true", help="Overwrite output file")
    return p.parse_args()

def main():
    args = parse_args()
    np.random.seed(args.seed)
    out = args.out

    if os.path.exists(out):
        if args.overwrite:
            os.remove(out)
        else:
            print(f"Error: {out} exists. Use --overwrite to replace.", file=sys.stderr)
            sys.exit(1)

    total_bytes_target = int(args.size_gb * 1024**3)
    print(f"Generating HDF5 file '{out}' target≈{args.size_gb} GB ({human(total_bytes_target)})")
    # compute number of datasets
    total_ds = args.branches * args.subgroups * args.datasets_per_subgroup
    print(f"Will create {total_ds} datasets across {args.branches} branches x {args.subgroups} subgroups x {args.datasets_per_subgroup} datasets")

    # allocate sizes per dataset (in bytes) by sampling log-uniform between min and max
    min_b = args.min_ds_kb * 1024
    max_b = args.max_ds_mb * 1024 * 1024
    sizes = np.exp(np.random.uniform(np.log(min_b), np.log(max_b), size=total_ds)).astype(int)

    # scale sizes to reach target total
    cur_total = sizes.sum()
    scale = total_bytes_target / max(cur_total, 1)
    sizes = (sizes * scale).astype(int)
    # enforce bounds
    sizes = np.clip(sizes, min_b, max_b)

    # recalc
    cur_total = sizes.sum()
    print(f"Total dataset bytes to create: {human(cur_total)}")

    # Create file and hierarchical structure
    libver = "latest"
    with h5py.File(out, "w", libver=libver) as f:
        # top-level attributes to simulate metadata dicts
        f.attrs['creator'] = "gen_h5_str.py"
        f.attrs['target_bytes'] = total_bytes_target
        idx = 0
        pbar = tqdm(total=total_ds, desc="Creating datasets")
        for b in range(args.branches):
            br_name = f"branch_{b}"
            grp_b = f.create_group(br_name)
            grp_b.attrs['branch_note'] = f"Branch {b} contains synthetic datasets for benchmarking."
            for s in range(args.subgroups):
                sub_name = f"sub_{s}"
                grp_s = grp_b.create_group(sub_name)
                grp_s.attrs['sub_info'] = f"Subgroup {s}"
                for d in range(args.datasets_per_subgroup):
                    ds_name = f"ds_{d}"
                    ds_bytes = int(sizes[idx])
                    # choose shape: we'll write float32 arrays; compute number of elements
                    dtype = np.float32
                    elem_size = np.dtype(dtype).itemsize
                    nelems = max(1, ds_bytes // elem_size)
                    # choose shape 2D or 1D
                    # prefer row length around 1024
                    rows = max(1, nelems // 1024)
                    cols = max(1, nelems // rows)
                    shape = (rows, cols)
                    # chunking: try to have chunk around chunks-kb parameter
                    target_chunk_bytes = args.chunks_kb * 1024
                    # compute chunk dims
                    chunk_elems = max(1, target_chunk_bytes // elem_size)
                    chunk_rows = max(1, min(rows, int(math.sqrt(chunk_elems))))
                    chunk_cols = max(1, int(chunk_elems // chunk_rows))
                    chunk_shape = (chunk_rows, chunk_cols)
                    # create dataset
                    dset = grp_s.create_dataset(ds_name, shape=shape, maxshape=(None, None),
                                                dtype=dtype, chunks=chunk_shape)
                    # add attributes to simulate dict metadata
                    dset.attrs['created_by'] = 'gen_h5_str.py'
                    dset.attrs['sim_year'] = 1900 + (idx % 120)
                    dset.attrs['has_vocal'] = bool(idx % 2)
                    # write random data in small blocks to avoid high memory
                    # write row-by-row or in blocks of 1024 rows
                    block_rows = 256
                    written = 0
                    total_rows = shape[0]
                    for start in range(0, total_rows, block_rows):
                        end = min(total_rows, start + block_rows)
                        block_shape = (end - start, shape[1])
                        arr = np.random.rand(*block_shape).astype(dtype)
                        dset[start:end, :] = arr
                        written += arr.nbytes
                    idx += 1
                    pbar.update(1)
        pbar.close()
        # add a group that simulates lists and dicts as nested keys + variable-length strings
        meta_grp = f.create_group("metadata_collections")
        meta_grp.attrs['note'] = "Collections: lists and dict-like structures using groups and datasets."
        # create several "list" groups containing variable-length strings
        vlen_dt = h5py.string_dtype(encoding='utf-8')
        for i in range(10):
            lg = meta_grp.create_group(f"list_{i}")
            # create a dataset of strings
            entries = [f"item_{i}_{j}" for j in range(1000)]
            lg.create_dataset("items", data=np.array(entries, dtype=object), dtype=vlen_dt)
        # create a nested dict-like group
        dict_grp = meta_grp.create_group("big_dict")
        for k in range(500):
            sub = dict_grp.create_group(f"key_{k}")
            sub.attrs['type'] = "dict_entry"
            sub.create_dataset("value", data=np.random.randint(0, 1<<30, size=(128,)), dtype=np.int64)
    print("Done. File written:", out)
    print("Actual file size:", human(os.path.getsize(out)))

if __name__ == "__main__":
    main()

