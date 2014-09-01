from argparse import ArgumentParser
from multiprocessing import Pool
from mrdomino import map_one_shard, logger
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
    ap.add_argument('--input_files', type=str, nargs='+',
                    help='input files')
    ap.add_argument('--map_module', type=str, required=True,
                    help='path to module containing mapper')
    ap.add_argument('--map_func', type=str, required=True,
                    help='mapper function name')
    ap.add_argument('--combine_module', type=str, required=False,
                    help='path to module containing combiner')
    ap.add_argument('--combine_func', type=str, required=False,
                    help='combiner function name')
    ap.add_argument('--work_dir', type=str,
                    help='directory containing map output files')
    ap.add_argument('--output_prefix', type=str, default='map.out',
                    help='string to prefix output files')
    args = ap.parse_args()
    return args


def do_shard(t):

    # unwrap argument
    args, shard = t

    with MRTimer() as timer:
        map_one_shard.map(shard, args)
    logger.info("Shard {} mapped: {}".format(shard, str(timer)))


def main():
    args = parse_args()

    shards = map(int, args.shards.split(','))
    logger.info("Scheduling shards {} on one mapper node".format(shards))
    pool = Pool(processes=len(shards))

    # Note: wrapping arguments to do_shard into a tuple since multiprocessing
    # does not support map functions with >1 argument and using a lambda
    # will result in a pickling error since python's pickle is horrible
    pool.map(do_shard, [(args, shard) for shard in shards])


if __name__ == '__main__':
    main()
