## HDF5 benchmarks with MPI support
This repository contains necessary script to perform
HDF5 benchmark with auto-generated h5 files.

### Installation instructions
```
# install miniforge3
curl -ksLO https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh
bash Miniforge3-Linux-x86_64.sh
source /path/miniforge3/bin/activate

# create new environment
mamba env create -f ./environment.yaml

# check MPI environment
python -c "import h5py; print('MPI-enabled:', h5py.get_config().mpi)"
MPI-enabled: True
```

### HDF5 benchmarks
To run benchmark you can use either `example.py` or `run_bench.sh` scripts.
For instance:
```
# generate 2GB file and run swrm vs mpi parallel benchmark with 16 shards split
./run_bench.sh --size-gb 2 --file big.h5 --mpirun "-n 16"
```
Here is results of using 2GB file:
```
Generating HDF5 file: big.h5 (approx 2 GB)
Generating HDF5 file 'big.h5' target≈2.0 GB (2.0GB)
Will create 128 datasets across 4 branches x 8 subgroups x 4 datasets
Total dataset bytes to create: 1.7GB
Done. File written: big.h5
Actual file size: 1.7GB

Running SWMR benchmark (single-process reader)...
Results saved to swmr_results.json
SWMR results -> swmr_results.json
SWMR /usr/bin/time output saved to swmr_time.log
	Command being timed: "python3 h5_swmr_bench.py --file big.h5 --sample-reads 200 --out swmr_results.json"
	User time (seconds): 15.08
	System time (seconds): 2.87
	Percent of CPU this job got: 63%
	Elapsed (wall clock) time (h:mm:ss or m:ss): 0:28.29
	Average shared text size (kbytes): 0
	Average unshared data size (kbytes): 0
	Average stack size (kbytes): 0
	Average total size (kbytes): 0
	Maximum resident set size (kbytes): 215252
	Average resident set size (kbytes): 0
	Major (requiring I/O) page faults: 2
	Minor (reclaiming a frame) page faults: 27428
	Voluntary context switches: 391143
	Involuntary context switches: 685
	Swaps: 0
	File system inputs: 3668104
	File system outputs: 120
	Socket messages sent: 0
	Socket messages received: 0
	Signals delivered: 0
	Page size (bytes): 4096
	Exit status: 0

Running mpar benchmark with mpirun -n 16 ...
MPI rank=10 size=16
MPI rank=8 size=16
MPI rank=12 size=16
MPI rank=4 size=16
MPI rank=1 size=16
MPI rank=5 size=16
MPI rank=6 size=16
MPI rank=3 size=16
MPI rank=0 size=16
MPI rank=15 size=16
MPI rank=14 size=16
MPI rank=13 size=16
MPI rank=7 size=16
MPI rank=9 size=16
MPI rank=11 size=16
MPI rank=2 size=16
mpar benchmark results written to mpar_results.json
mpar results -> mpar_results.json
MPI run stderr saved to mpar_time.log

Post-processing: summary prints
SWMR summary:
  file: big.h5
  samples: 1
  summary: {'proc_read_delta': 1877962752, 'sys_read_delta': 0, 'duration': 23.626959323883057}

Parallel summary:
  file: big.h5
   rank 0: assigned 40 datasets, read_full_bytes=31.5MB, full_time=21.324s
   rank 1: assigned 40 datasets, read_full_bytes=64.3MB, full_time=41.892s
   rank 2: assigned 40 datasets, read_full_bytes=139.6MB, full_time=63.342s
   rank 3: assigned 40 datasets, read_full_bytes=78.6MB, full_time=46.850s
   rank 4: assigned 40 datasets, read_full_bytes=245.0MB, full_time=75.304s
   rank 5: assigned 40 datasets, read_full_bytes=55.3MB, full_time=39.410s
   rank 6: assigned 40 datasets, read_full_bytes=207.6MB, full_time=73.656s
   rank 7: assigned 40 datasets, read_full_bytes=174.3MB, full_time=70.531s
   rank 8: assigned 40 datasets, read_full_bytes=164.2MB, full_time=69.586s
   rank 9: assigned 40 datasets, read_full_bytes=95.6MB, full_time=54.210s
   rank 10: assigned 40 datasets, read_full_bytes=135.9MB, full_time=64.654s
   rank 11: assigned 40 datasets, read_full_bytes=96.8MB, full_time=53.864s
   rank 12: assigned 40 datasets, read_full_bytes=25.7MB, full_time=24.610s
   rank 13: assigned 40 datasets, read_full_bytes=166.2MB, full_time=69.925s
   rank 14: assigned 39 datasets, read_full_bytes=16.5MB, full_time=19.303s
   rank 15: assigned 39 datasets, read_full_bytes=66.7MB, full_time=42.035s
Done.
```
