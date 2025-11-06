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
