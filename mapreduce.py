#!/usr/bin/python

from argparse import ArgumentParser
import imp
import os
import time

ap = ArgumentParser()
ap.add_argument('--input_files', type=str, nargs='+',
                help='list of input files to mappers')

ap.add_argument('--mr', type=str,
                help='path to module containing map() and reduce() to use')

ap.add_argument('--n_map_shards', type=int, default=20,
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

print args.input_files

# verify that we have input for each mapper.
assert args.n_map_shards <= len(args.input_files)

# verify the --mr module (containing user's map/reduce functions) exists.
module = imp.load_source('module', args.mr)


def wipe_done_files():
    # remove mapper 'done' files.
    for i in range(args.n_map_shards):
        f = 'mapreduce/map.%d.done' % i
        if os.path.exists(f):
            os.remove(f)

    for i in range(args.n_reduce_shards):
        f = 'mapreduce/reduce.%d.done' % i
        if os.path.exists(f):
            os.remove(f)


def is_map_step_done():
    for i in range(args.n_map_shards):
        f = 'mapreduce/map.%d.done' % i
        if not os.path.isfile(f):
            return False
    return True


def is_reduce_step_done():
    for i in range(args.n_reduce_shards):
        f = 'mapreduce/reduce.%d.done' % i
        if not os.path.isfile(f):
            return False
    return True


def main():
    wipe_done_files()

    # run mappers.
    print 'Starting mappers.'
    for i in range(args.n_map_shards):
        if args.use_domino:
            pre = 'domino run '
            post = ''
        else:
            pre = ''
            post = ' &'
        cmd = """%spython mapreduce/map.py \
            --shard %d \
            --n_shards %d \
            --input_files %s \
            --mr %s%s""" % (
            pre, i, args.n_map_shards, ' '.join(args.input_files), args.mr, post)
        os.system(cmd)

    # wait for mappers to complete.
    while not is_map_step_done():
        print 'Polling for mapper completion.'
        time.sleep(args.poll_done_interval_sec)
        if args.use_domino:
            os.system('domino download')

    # shuffle mapper outputs to reducer inputs.
    print 'Shuffling data.'
    cmd = """python mapreduce/shuffle.py \
        --n_map_shards %d \
        --n_reduce_shards %d
    """ % (args.n_map_shards, args.n_reduce_shards)
    os.system(cmd)

    # run reducers.
    print 'Starting reducers.'
    for i in range(args.n_reduce_shards):
        if args.use_domino:
            pre = 'domino run '
            post = ''
        else:
            pre = ''
            post = ' &'
        cmd = """%spython mapreduce/reduce.py \
            --shard %d \
            --n_shards %d \
            --mr %s%s""" % (pre, i, args.n_reduce_shards, args.mr, post)
        os.system(cmd)

    # wait for reducers to complete.
    while not is_reduce_step_done():
        print 'Polling for reducer completion.'
        time.sleep(args.poll_done_interval_sec)
        if args.use_domino:
            os.system('domino download')

    wipe_done_files()

    # done.
    print 'Mapreduce done.'


if __name__ == '__main__':
    main()
