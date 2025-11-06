import multiprocessing as mp
import numpy as np
import h5py
import time
import os


FILE_NAME = "swmr_demo.h5"
DATASET_NAME = "values"


def writer_process():
    """Continuously appends data to an HDF5 dataset."""
    print("[Writer] Starting writer process...")

    # Create a new HDF5 file
    with h5py.File(FILE_NAME, "w", libver="latest") as f:
        # Create resizable (extendable) dataset, chunked as required for SWMR
        dset = f.create_dataset(
            DATASET_NAME,
            shape=(0,),
            maxshape=(None,),
            dtype="f",
            chunks=(100,),
        )

        for i in range(10):
            # Simulate new data block
            new_data = np.random.random(100)
            old_size = dset.shape[0]
            new_size = old_size + new_data.size

            # Resize and append
            dset.resize((new_size,))
            dset[old_size:] = new_data

            # Flush data so SWMR readers can see it
            f.flush()
            print(f"[Writer] Wrote block {i+1}, total size={new_size}")
            time.sleep(1)  # simulate time delay between writes

    print("[Writer] Finished writing.")


def reader_process():
    """Continuously reads and prints dataset size as new data arrives."""
    print("[Reader] Waiting for file...")
    while not os.path.exists(FILE_NAME):
        time.sleep(0.2)

    print("[Reader] Opening file in SWMR mode...")
    with h5py.File(FILE_NAME, "r", swmr=True) as f:
        dset = f[DATASET_NAME]
        last_size = 0

        for _ in range(15):  # poll multiple times
            f.refresh()  # refresh view of file for new data
            new_size = dset.shape[0]
            if new_size > last_size:
                print(f"[Reader] New data detected! Size={new_size}")
                # Print last few values to demonstrate live read
                print(f"[Reader] Last 5 values: {dset[-5:]}")
                last_size = new_size
            time.sleep(0.7)

    print("[Reader] Done.")


if __name__ == "__main__":
    # Remove old file if present
    try:
        os.remove(FILE_NAME)
    except FileNotFoundError:
        pass

    writer = mp.Process(target=writer_process)
    reader = mp.Process(target=reader_process)

    # Start reader first, then writer
    reader.start()
    time.sleep(0.5)
    writer.start()

    writer.join()
    reader.join()

