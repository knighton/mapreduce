#!/usr/bin/python

from argparse import ArgumentParser
import collections
import imp
import json
import os

import mrd_util

ap = ArgumentParser()
ap.add_argument('--shards', type=str,
                help='which shards we are')
ap.add_argument('--n_shards', type=int,
                help='out of how many shards')
ap.add_argument('--reduce_module', type=str,
                help='path to module containing reducer')
ap.add_argument('--reduce_func', type=str,
                help='reduce function name')
ap.add_argument('--work_dir', type=str,
                help='directory containing reduce input files')
ap.add_argument('--output_dir', type=str, default='.',
                help='directory containing reduce output files')
args = ap.parse_args()


# get/check shards.
shards = map(int, args.shards.split(','))
for shard in shards:
    assert 0 <= shard < args.n_shards
assert args.work_dir


# find the reduce function.
reduce_module = imp.load_source('reduce_module', args.reduce_module)
reduce_func = getattr(reduce_module, args.reduce_func)


# execute each shard on this machine.
for shard in shards:
    cmd = """
python mrd_reduce_one_shard.py \
    --shard %d \
    --n_shards %d \
    --reduce_module %s \
    --reduce_func %s \
    --work_dir %s \
    --output_dir %s &""" % (
        shard, args.n_shards, args.reduce_module, args.reduce_func,
        args.work_dir, args.output_dir)
    os.system(cmd)
