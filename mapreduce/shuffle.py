#!/usr/bin/python

from argparse import ArgumentParser
import glob
import heapq
import os

ap = ArgumentParser()
ap.add_argument('--work_dir', type=str,
                help='directory containing files to shuffle')
ap.add_argument('--n_reduce_shards', type=int,
                help='number of reducers to create input files for')
args = ap.parse_args()

assert args.work_dir
assert args.n_reduce_shards

num_entries = 0
in_ff = sorted(glob.glob('%s/map.out.*' % args.work_dir))
sources = []
for f in in_ff:
    for line in open(f):
        num_entries += 1
    print 'Sorting %s' % f
    os.system('sort %s -o %s' % (f, f))
    sources.append(open(f))

print 'Merging'
out_ff = map(lambda i: open('mapreduce/tmp/reduce.in.%d' % i, 'w'),
             range(args.n_reduce_shards))
count = 0
for line in heapq.merge(*sources):
    index = count * len(out_ff) / num_entries
    out_ff[index].write(line)
    count += 1
