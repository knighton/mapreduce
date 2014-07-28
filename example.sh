#!/bin/sh

python mapreduce.py --input_files data/* --n_map_shards 4 --n_reduce_shards 10 --mr example.py
