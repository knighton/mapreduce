from argparse import ArgumentParser
from multiprocessing import Pool
from mrdomino import reduce_one_shard, logger
from mrdomino.util import MRTimer


def parse_args():
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
    ap.add_argument('--input_prefix', type=str, default=None,
                    help='string that input files are prefixed with')
    ap.add_argument('--glob_prefix', type=str, default=None,
                    help='string that input files are prefixed with '
                    'Using this option instead of input_prefix will '
                    'cause all files to be read at once')
    ap.add_argument('--output_prefix', type=str, default='reduce.out',
                    help='string to prefix output files')
    args = ap.parse_args()
    return args


def do_shard(t):

    # unwrap argument
    args, shard = t

    with MRTimer() as timer:
        reduce_one_shard.reduce(shard, args)
    logger.info("Shard {} reduced: {}".format(shard, str(timer)))


def main():
    args = parse_args()

    shards = map(int, args.shards.split(','))
    logger.info("Scheduling shards {} on one reducer node".format(shards))
    pool = Pool(processes=len(shards))

    # Note: wrapping arguments to do_shard into a tuple since multiprocessing
    # does not support map functions with >1 argument and using a lambda
    # will result a pickling error since python's pickle is horrible
    pool.map(do_shard, [(args, shard) for shard in shards])

if __name__ == '__main__':
    main()
