#!/usr/bin/python

from argparse import ArgumentParser
from multiprocessing import Pool

import mrd_reduce_one_shard

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


def do_shard(shard):
    mrd_reduce_one_shard.reduce(
        args.reduce_module, args.reduce_func, args.work_dir, args.output_dir,
        shard)


shards = map(int, args.shards.split(','))
pool = Pool(processes=len(shards))
pool.map(do_shard, shards)
