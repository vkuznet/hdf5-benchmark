#!/usr/bin/env python3
"""
gen_h5_zar.py
Creates a large, complex HDF5 file for I/O benchmarking.

Usage:
    python gen_h5_zar.py --output test.h5 --size 2GB
"""

import argparse
import os
import numpy as np
import h5py
import zarr
from tqdm import tqdm

def parse_size(size_str):
    """Parse human-readable size like '500MB' or '2GB'."""
    units = {"kb": 1e3, "mb": 1e6, "gb": 1e9}
    size_str = size_str.lower().strip()
    for unit, factor in units.items():
        if size_str.endswith(unit):
            return int(float(size_str[:-len(unit)]) * factor)
    return int(size_str)

def create_complex_structure(h5file, total_bytes):
    """Create a nested structure with arrays, lists, and metadata."""
    rng = np.random.default_rng(42)
    total_written = 0
    chunk_size = 1024 * 1024  # 1 MB
    group = h5file.create_group("root")

    for i in tqdm(range(10), desc="Creating groups"):
        g = group.create_group(f"group_{i}")
        g.attrs["description"] = f"Group {i} metadata"
        g.attrs["params"] = {"alpha": i * 0.1, "beta": np.sin(i)}

        for j in range(5):
            dshape = (256, 256)
            dset = g.create_dataset(
                f"data_{j}",
                shape=dshape,
                dtype="float32",
                compression="gzip",
                chunks=True,
            )
            data = rng.random(dshape, dtype=np.float32)
            dset[:] = data
            total_written += data.nbytes
            if total_written > total_bytes:
                return total_written

    return total_written

def create_h5_file(filename, size_bytes):
    with h5py.File(filename, "w") as h5file:
        written = create_complex_structure(h5file, size_bytes)
    print(f"✅ HDF5 file '{filename}' created ({written/1e6:.2f} MB).")

def create_zarr_file(filename, size_bytes):
    store = zarr.DirectoryStore(filename)
    root = zarr.group(store=store)
    rng = np.random.default_rng(42)
    total_written = 0

    for i in tqdm(range(10), desc="Creating Zarr groups"):
        g = root.create_group(f"group_{i}")
        for j in range(5):
            dshape = (256, 256)
            arr = g.create_dataset(
                f"data_{j}", shape=dshape, dtype="float32", compressor=zarr.get_codec({"id": "zlib", "level": 5})
            )
            data = rng.random(dshape, dtype=np.float32)
            arr[:] = data
            total_written += data.nbytes
            if total_written > size_bytes:
                break

    print(f"✅ Zarr dataset '{filename}' created ({total_written/1e6:.2f} MB).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate large HDF5/Zarr dataset for benchmarking.")
    parser.add_argument("--output", required=True, help="Output file path (.h5 or .zarr)")
    parser.add_argument("--size", default="1GB", help="Approximate total size (e.g., 500MB, 2GB)")
    args = parser.parse_args()

    size_bytes = parse_size(args.size)
    ext = os.path.splitext(args.output)[1].lower()

    if ext == ".h5":
        create_h5_file(args.output, size_bytes)
    elif ext == ".zarr":
        create_zarr_file(args.output, size_bytes)
    else:
        raise ValueError("Unsupported file extension. Use .h5 or .zarr")

