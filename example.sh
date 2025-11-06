# create a small test file (0.1 GB)
python3 gen_h5_num.py --out test_small.h5 --size-gb 0.1 --branches 2 --subgroups 4 --datasets-per-subgroup 2 --overwrite

# run SWMR bench
#python3 h5_swmr_bench.py --file test_small.h5 --sample-reads 20 --out swmr_small.json
python3 h5_mpar_bench.py --file test_small.h5 --sample-reads 20 --out swmr_small.json --method=swmr

# run mpar bench in serial (if mpirun not available)
python3 h5_mpar_bench.py --file test_small.h5 --sample-reads 20 --out mpar_small.json

# get statistics
python3 stats.py --swmr=swmr_small.json --mpar=mpar_small.json
