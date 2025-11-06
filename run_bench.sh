#!/usr/bin/env bash
# run_bench.sh
# Orchestrate generation + SWMR benchmark + MPI benchmark, collect logs.
# Usage:
#   ./run_bench.sh --size-gb 2 --file big.h5 --mpirun "-n 4" --gen-script=gen_h5_num.py
set -euo pipefail

# defaults
SIZE_GB=1
OUT_FILE="big_test.h5"
BRANCHES=4
SUBGROUPS=8
DS_PER_SUB=4
MPIRUN_OPTS="-n 4"
SWMR_OUT="swmr_results.json"
MPAR_OUT="mpar_results.json"
SAMPLE_READS=200
GEN_SCRIPT=gen_h5_num.py

print_usage() {
  cat <<EOF
Usage: $0 [options]
Options:
  --size-gb N            target size in GB (default ${SIZE_GB})
  --file NAME            output h5 file (default ${OUT_FILE})
  --mpirun "opts"        options for mpirun (default ${MPIRUN_OPTS})
  --swmr-out FILE        swmr JSON output (default ${SWMR_OUT})
  --mpar-out FILE        mpar JSON output (default ${MPAR_OUT})
  --gen-script FILE      generate file (default ${GEN_SCRIPT})
EOF
}

# parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --size-gb) SIZE_GB="$2"; shift 2;;
    --file) OUT_FILE="$2"; shift 2;;
    --mpirun) MPIRUN_OPTS="$2"; shift 2;;
    --swmr-out) SWMR_OUT="$2"; shift 2;;
    --mpar-out) MPAR_OUT="$2"; shift 2;;
    --gen-script) GEN_SCRIPT="$2"; shift 2;;
    --help) print_usage; exit 0;;
    *) echo "Unknown arg: $1"; print_usage; exit 1;;
  esac
done

echo "Generating HDF5 file: ${OUT_FILE} (approx ${SIZE_GB} GB)"
python3 gen_h5_num.py --out "${OUT_FILE}" --size-gb "${SIZE_GB}" --branches ${BRANCHES} --subgroups ${SUBGROUPS} --datasets-per-subgroup ${DS_PER_SUB} --overwrite

echo
echo "Running SWMR benchmark (single-process reader)..."
# Use /usr/bin/time -v if available to report resource usage
if command -v /usr/bin/time >/dev/null 2>&1; then
  #/usr/bin/time -v python3 h5_swmr_bench.py --file "${OUT_FILE}" --sample-reads $SAMPLE_READS --out "${SWMR_OUT}" 2> swmr_time.log || true
  /usr/bin/time -v python3 h5_mpar_bench.py --method=swmr --file "${OUT_FILE}" --sample-reads $SAMPLE_READS --out "${SWMR_OUT}" 2> swmr_time.log || true
else
  #python3 h5_swmr_bench.py --file "${OUT_FILE}" --sample-reads $SAMPLE_READS --out "${SWMR_OUT}"
  python3 h5_mpar_bench.py --method=swmr --file "${OUT_FILE}" --sample-reads $SAMPLE_READS --out "${SWMR_OUT}"
fi

echo "SWMR results -> ${SWMR_OUT}"
[ -f swmr_time.log ] && echo "SWMR /usr/bin/time output saved to swmr_time.log" && cat swmr_time.log


echo
echo "Running mpar benchmark with mpirun ${MPIRUN_OPTS} ..."
# run MPI benchmark
# If mpirun is available, run. Otherwise just run single-process fallback.
if command -v mpirun >/dev/null 2>&1; then
  # redirect each rank output to prefix
  mpirun ${MPIRUN_OPTS} python3 h5_mpar_bench.py --file "${OUT_FILE}" --sample-reads $SAMPLE_READS --out "${MPAR_OUT}" 2> mpar_time.log || true
else
  echo "mpirun not found — running mpar script in serial as fallback"
  python3 h5_mpar_bench.py --file "${OUT_FILE}" --sample-reads $SAMPLE_READS --out "${MPAR_OUT}"
fi

echo "mpar results -> ${MPAR_OUT}"
[ -f mpar_time.log ] && echo "MPI run stderr saved to mpar_time.log" && cat mpar_time.log

echo
echo "Post-processing: summary prints"
python3 stats.py --swmr=${SWMR_OUT} --mpar=${MPAR_OUT}
echo "Done."

