#!/usr/bin/python

from argparse import ArgumentParser
import imp
import os

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

# get/check shards.
shards = map(int, args.shards.split(','))
for shard in shards:
    assert 0 <= shard < args.n_shards
assert args.work_dir


# find/check the map function.
map_module = imp.load_source('map_module', args.map_module)
map_func = getattr(map_module, args.map_func)


# execute each shard on this machine.
for shard in shards:
    cmd = """
python mrd_map_one_shard.py \
    --shard %d \
    --n_shards %d \
    --input_files %s \
    --map_module %s \
    --map_func %s \
    --work_dir %s &""" % (
        shard, args.n_shards, ' '.join(args.input_files), args.map_module,
        args.map_func, args.work_dir)
    os.system(cmd)
