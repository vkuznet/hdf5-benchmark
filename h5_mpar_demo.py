from mpi4py import MPI
import numpy as np
import h5py
import time


comm = MPI.COMM_WORLD
rank = comm.rank
size = comm.size

FILE_NAME = "parallel_demo.h5"


def parallel_write():
    """Each MPI process writes its own dataset slice concurrently."""
    # Open file collectively using MPI driver
    with h5py.File(FILE_NAME, "w", driver="mpio", comm=comm) as f:
        # Create dataset with one row per process
        dset = f.create_dataset("data", (size, 1000), dtype="f")

        # Each rank fills its row independently
        data = np.arange(1000, dtype="f") + rank * 1000
        dset[rank, :] = data
        print(f"[Rank {rank}] wrote data from {data[0]} to {data[-1]}")

    comm.Barrier()
    if rank == 0:
        print("[All ranks] Finished writing.")


def parallel_read():
    """Each MPI process reads its own dataset slice."""
    with h5py.File(FILE_NAME, "r", driver="mpio", comm=comm) as f:
        dset = f["data"]
        data = dset[rank, :]
        print(f"[Rank {rank}] read sum = {data.sum():.0f}")

    comm.Barrier()
    if rank == 0:
        print("[All ranks] Finished reading.")


if __name__ == "__main__":
    t0 = time.time()
    parallel_write()
    comm.Barrier()
    parallel_read()
    comm.Barrier()
    if rank == 0:
        print(f"Total elapsed: {time.time() - t0:.2f}s")

