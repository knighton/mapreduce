#!/usr/bin/env python2

from argparse import ArgumentParser
import glob
import heapq

ap = ArgumentParser()
ap.add_argument('--work_dir', type=str,
                help='directory containing files to shuffle')
ap.add_argument('--n_reduce_shards', type=int,
                help='number of reducers to create input files for')
args = ap.parse_args()

assert args.work_dir
assert args.n_reduce_shards

# count exactly how many input lines we have so we can balance work.
num_entries = 0
count_ff = glob.glob('%s/map.out_count.*' % args.work_dir)
for f in count_ff:
    n = int(open(f).read())
    num_entries += n

in_ff = sorted(glob.glob('%s/map.out.*' % args.work_dir))
sources = map(open, in_ff)

out_ff = map(lambda i: open('%s/reduce.in.%d' % (args.work_dir, i), 'w'),
             range(args.n_reduce_shards))

count = 0
for line in heapq.merge(*sources):
    index = count * len(out_ff) / num_entries
    out_ff[index].write(line)
    count += 1
