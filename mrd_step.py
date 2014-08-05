#!/usr/bin/python

from argparse import ArgumentParser
import collections
import imp
import os
import random
import string
import time

import mrd_util

ap = ArgumentParser()
ap.add_argument('--input_files', type=str, nargs='+',
                help='list of input files to mappers')
ap.add_argument('--output_dir', type=str, default='.',
                help='directory to write output files to')

ap.add_argument('--map_module', type=str)
ap.add_argument('--map_func', type=str)
ap.add_argument('--reduce_module', type=str)
ap.add_argument('--reduce_func', type=str)

ap.add_argument('--n_map_shards', type=int,
                help='number of map shards')
ap.add_argument('--n_reduce_shards', type=int, default=10,
                help='number of reduce shards')
ap.add_argument('--n_concurrent_jobs', type=int, default=2,
                help='maximum number of domino jobs to be running at once')

ap.add_argument('--poll_done_interval_sec', type=int, default=45,
                help='interval between successive checks that we are done')

ap.add_argument('--use_domino', type=int, default=1,
                help='which platform to run on (local or domino)')

args = ap.parse_args()


# verify that we have input for each mapper.
if args.n_map_shards is None:
    args.n_map_shards = len(args.input_files)


# verify functions exist.
module = imp.load_source('module', args.map_module)
func = getattr(module, args.map_func)
module = imp.load_source('module', args.reduce_module)
func = getattr(module, args.reduce_func)


def wrap_cmd(cmd, use_domino):
    if use_domino:
        pre = 'domino run '
        post = ''
    else:
        pre = 'python '
        post = ' &'
    return '%s%s%s' % (pre, cmd, post)


class ShardState(object):
    NOT_STARTED = 0
    IN_PROGRESS = 1
    DONE = 2


def update_shards_done(done_pattern, num_shards, use_domino, shard2state):
    """go to disk and find out which shards are completed."""
    if args.use_domino:
        os.system('domino download')
    for i in range(num_shards):
        f = done_pattern % i
        if os.path.isfile(f):
            shard2state[i] = ShardState.DONE


def are_all_shards_done(shard2state):
    return list(set(shard2state.itervalues())) == [ShardState.DONE]


def flip_dict(k2v):
    """(k -> v) -> (v -> kk)."""
    v2kk = collections.defaultdict(list)
    for k, v in k2v.iteritems():
        v2kk[v].append(k)
    return v2kk


def update_shards_in_progress(n_concurrent_jobs, shard2state):
    """get the list of shards to start now.  update state accordingly."""
    state2shards = flip_dict(shard2state)
    n_todos = n_concurrent_jobs - len(state2shards[ShardState.IN_PROGRESS])
    todos = state2shards[ShardState.NOT_STARTED][:n_todos]
    for todo in todos:
        shard2state[todo] = ShardState.IN_PROGRESS
    return todos


def run_shards(cmd, n_shards, n_concurrent_jobs, poll_done_interval_sec,
               done_file_pattern, use_domino):
    # shard -> state
    # 0: not started
    # 1: in progress
    # 2: completed
    shard2state = dict(zip(
        range(n_shards),
        [ShardState.NOT_STARTED] * n_shards))

    while True:
        print 'Checking for shard completion.'
        update_shards_done(done_file_pattern, n_shards, use_domino, shard2state)
        if are_all_shards_done(shard2state):
            break

        start_me = update_shards_in_progress(n_concurrent_jobs, shard2state)
        if start_me:
            print 'Starting shards', start_me
        for shard in start_me:
            s = cmd % shard
            s = wrap_cmd(s, use_domino)
            os.system(s)

        time.sleep(poll_done_interval_sec)


def random_string(length):
    choices = string.ascii_lowercase + string.ascii_uppercase + string.digits
    cc = []
    for i in range(length):
        cc.append(random.choice(choices))
    return ''.join(cc)


def main():
    print '%d input files.' % len(args.input_files)

    # create temporary working directory.
    work_dir = 'mapreduce/tmp/%s' % random_string(16)
    if os.path.exists(work_dir):
        os.system('rm -rf %s' % work_dir)
    os.makedirs(work_dir)
    print 'Working directory: %s' % work_dir

    print 'Starting %d mappers.' % args.n_map_shards
    cmd = """mrd_map.py \
        --shard %%d \
        --n_shards %d \
        --input_files %s \
        --map_module %s \
        --map_func %s \
        --work_dir %s""" % (
        args.n_map_shards, ' '.join(args.input_files), args.map_module,
        args.map_func, work_dir)
    done_file_pattern = '%s/map.done.%%d' % work_dir
    run_shards(cmd, args.n_map_shards, args.n_concurrent_jobs,
               args.poll_done_interval_sec, done_file_pattern, args.use_domino)

    ff = map(lambda (work_dir, shard): '%s/map.counters.%d' % (work_dir, shard),
             zip([work_dir] * args.n_map_shards,
                 range(args.n_map_shards)))
    mrd_util.show_combined_counters(ff)

    # shuffle mapper outputs to reducer inputs.
    print 'Shuffling data.'
    cmd = """python mrd_shuffle.py \
        --work_dir %s \
        --n_reduce_shards %d
    """ % (work_dir, args.n_reduce_shards)
    os.system(cmd)

    print 'Starting %d reducers.' % args.n_reduce_shards
    cmd = """mrd_reduce.py \
        --shard %%d \
        --n_shards %d \
        --reduce_module %s \
        --reduce_func %s \
        --work_dir %s \
        --output_dir %s""" % (
        args.n_reduce_shards, args.reduce_module, args.reduce_func, work_dir,
        args.output_dir)
    done_file_pattern = '%s/reduce.done.%%d' % work_dir
    run_shards(cmd, args.n_reduce_shards, args.n_concurrent_jobs,
               args.poll_done_interval_sec, done_file_pattern, args.use_domino)

    ff = map(lambda (work_dir, shard): '%s/map.counters.%d' % (work_dir, shard),
             zip([work_dir] * args.n_map_shards,
                 range(args.n_map_shards)))
    ff += map(lambda (work_dir, shard): '%s/reduce.counters.%d' % (work_dir, shard),
              zip([work_dir] * args.n_reduce_shards,
                  range(args.n_reduce_shards)))
    mrd_util.show_combined_counters(ff)

    # done.
    print 'Mapreduce step done.'


if __name__ == '__main__':
    main()
