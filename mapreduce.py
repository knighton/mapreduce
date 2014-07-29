#!/usr/bin/python

from argparse import ArgumentParser
import collections
import imp
import os
import random
import string
import time

ap = ArgumentParser()
ap.add_argument('--input_files', type=str, nargs='+',
                help='list of input files to mappers')
ap.add_argument('--output_dir', type=str, default='.',
                help='directory to write output files to')

ap.add_argument('--mr', type=str,
                help='path to module containing map() and reduce() to use')

ap.add_argument('--n_map_shards', type=int,
                help='number of map shards')
ap.add_argument('--n_reduce_shards', type=int, default=20,
                help='number of reduce shards')
ap.add_argument('--n_concurrent_jobs', type=int, default=2,
                help='maximum number of domino jobs to be running at once')

ap.add_argument('--poll_done_interval_sec', type=int, default=30,
                help='interval between successive checks that we are done')

ap.add_argument('--use_domino', type=int, default=1,
                help='which platform to run on (local or domino)')

args = ap.parse_args()

# verify that we have input for each mapper.
if args.n_map_shards is None:
    args.n_map_shards = len(args.input_files)
else:
    assert args.n_map_shards <= len(args.input_files)

# verify the --mr module (containing user's map/reduce functions) exists.
module = imp.load_source('module', args.mr)


def wrap_cmd(cmd, use_domino):
    if use_domino:
        pre = 'domino run '
        post = ''
    else:
        pre = ''
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
    cmd = """python mapreduce/map.py \
        --shard %%d \
        --n_shards %d \
        --input_files %s \
        --mr %s \
        --work_dir %s""" % (
        args.n_map_shards, ' '.join(args.input_files), args.mr, work_dir)
    done_file_pattern = '%s/map.done.%%d' % work_dir
    run_shards(cmd, args.n_map_shards, args.n_concurrent_jobs,
               args.poll_done_interval_sec, done_file_pattern, args.use_domino)

    # shuffle mapper outputs to reducer inputs.
    print 'Shuffling data.'
    cmd = """python mapreduce/shuffle.py \
        --work_dir %s \
        --n_reduce_shards %d
    """ % (work_dir, args.n_reduce_shards)
    os.system(cmd)

    print 'Starting %d reducers.' % args.n_reduce_shards
    cmd = """python mapreduce/reduce.py \
        --shard %%d \
        --n_shards %d \
        --mr %s \
        --work_dir %s \
        --output_dir %s""" % (
        args.n_reduce_shards, args.mr, work_dir, args.output_dir)
    done_file_pattern = '%s/reduce.done.%%d' % work_dir
    run_shards(cmd, args.n_reduce_shards, args.n_concurrent_jobs,
               args.poll_done_interval_sec, done_file_pattern, args.use_domino)

    # done.
    print 'Mapreduce done.'


if __name__ == '__main__':
    main()
