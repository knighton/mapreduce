#!/usr/bin/python

from argparse import ArgumentParser
from multiprocessing import Pool

import mrd_map_one_shard

ap = ArgumentParser()
ap.add_argument('--shards', type=str,
                help='which shards we are')
ap.add_argument('--n_shards', type=int,
                help='out of how many shards')
ap.add_argument('--input_files', type=str, nargs='+',
                help='input files')
ap.add_argument('--map_module', type=str,
                help='path to module containing mapper')
ap.add_argument('--map_func', type=str,
                help='mapper function name')
ap.add_argument('--work_dir', type=str,
                help='directory containing map output files')
args = ap.parse_args()


def do_shard(shard):
    mrd_map_one_shard.map(
        args.map_module, args.map_func, args.input_files, args.work_dir, shard,
        args.n_shards)


shards = map(int, args.shards.split(','))
pool = Pool(processes=len(shards))
pool.map(do_shard, shards)
