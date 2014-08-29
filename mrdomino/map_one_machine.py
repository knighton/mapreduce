from argparse import ArgumentParser
from multiprocessing import Pool

from mrdomino import map_one_shard

ap = ArgumentParser()
ap.add_argument('--step_idx', type=int, required=True,
                help='Index of this step (zero-base)')
ap.add_argument('--total_steps', type=int, required=True,
                help='total number of steps')
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
    map_one_shard.map(shard, args)


shards = map(int, args.shards.split(','))
print "Scheduling shards %s for system-level parallel map processing" \
    % shards
pool = Pool(processes=len(shards))
pool.map(do_shard, shards)
