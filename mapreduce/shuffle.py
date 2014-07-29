#!/usr/bin/python

from argparse import ArgumentParser
import heapq
import os

ap = ArgumentParser()
ap.add_argument('--n_map_shards', type=int, help='number of mapper shards')
ap.add_argument('--n_reduce_shards', type=int, help='number of reducer shards')
args = ap.parse_args()

num_entries = 0

sources = []
for i in range(args.n_map_shards):
    f = 'mapreduce/tmp/map.out.%d' % i
    for line in open(f):
        num_entries += 1
    os.system('sort %s' % f)
    sources.append(open(f))

out_ff = map(lambda i: open('mapreduce/tmp/reduce.in.%d' % i, 'w'),
             range(args.n_reduce_shards))
count = 0
for line in heapq.merge(*sources):
    index = count * len(out_ff) / num_entries
    out_ff[index].write(line)
    count += 1
