#!/bin/sh

python mapreduce.py \
    --input_files data/short.* \
    --n_map_shards 3 \
    --n_reduce_shards 5 \
    --map_module example.py \
    --map_func map \
    --reduce_module example.py \
    --reduce_func reduce \
    --n_concurrent_jobs 3 \
    --use_domino 0 \
    --output_dir data/
