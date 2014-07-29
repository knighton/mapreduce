#!/bin/sh

python mapreduce.py \
    --input_files data/*.detail.sorted \
    --n_map_shards 6 \
    --n_reduce_shards 10 \
    --mr example.py \
    --n_concurrent_jobs 4 \
    --use_domino 0 \
    --output_dir data/
