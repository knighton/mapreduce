import traceback
from StringIO import StringIO
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
    ap.add_argument('--job_module', type=str, required=True)
    ap.add_argument('--job_class', type=str, required=True)
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
    # Uses workaround to show traceback of uncaught exceptions (which by
    # default python's multiprocessing module fails to provide):
    # http://seasonofcode.com/posts/python-multiprocessing-and-exceptions.html
    try:
        args, shard = t  # unwrap argument
        with MRTimer() as timer:
            reduce_one_shard.reduce(shard, args)
        logger.info("Shard {} reduced: {}".format(shard, str(timer)))
    except Exception as e:
        exc_buffer = StringIO()
        traceback.print_exc(file=exc_buffer)
        logger.error('Uncaught exception while reducing shard {}. {}'
                     .format(shard, exc_buffer.getvalue()))
        raise e


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
