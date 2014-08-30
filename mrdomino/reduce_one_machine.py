from argparse import ArgumentParser
from multiprocessing import Pool

from mrdomino import reduce_one_shard

ap = ArgumentParser()
ap.add_argument('--step_idx', type=int, required=True,
                help='Index of this step (zero-base)')
ap.add_argument('--total_steps', type=int, required=True,
                help='total number of steps')
ap.add_argument('--shards', type=str,
                help='which shards we are')
ap.add_argument('--n_shards', type=int,
                help='out of how many shards')
ap.add_argument('--reduce_module', type=str,
                help='path to module containing reducer')
ap.add_argument('--reduce_func', type=str,
                help='reduce function name')
ap.add_argument('--work_dir', type=str, required=True,
                help='directory containing reduce input files')
ap.add_argument('--output_dir', type=str, default=None,
                help='directory containing reduce output files. '
                'If empty, will dump into work_dir')
ap.add_argument('--input_prefix', type=str, default='reduce.in',
                help='string that input files are prefixed with')
ap.add_argument('--glob_prefix', type=str, default=None,
                help='string that input files are prefixed with '
                'Using this option instead of input_prefix will '
                'cause all files to be read at once')
ap.add_argument('--output_prefix', type=str, default='reduce.out',
                help='string to prefix output files')
args = ap.parse_args()


def do_shard(shard):
    reduce_one_shard.reduce(shard, args)


shards = map(int, args.shards.split(','))
print "Scheduling shards %s for system-level parallel reduce processing" \
    % shards
pool = Pool(processes=len(shards))
pool.map(do_shard, shards)
