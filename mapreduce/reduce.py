#!/usr/bin/python

from argparse import ArgumentParser
import imp
import json

ap = ArgumentParser()
ap.add_argument('--shard', type=int,
                help='which shard we are')
ap.add_argument('--n_shards', type=int,
                help='out of how many shards')
ap.add_argument('--mr', type=str,
                help='path to module containing map() and reduce() to use')
args = ap.parse_args()

assert 0 <= args.shard < args.n_shards

module = imp.load_source('module', args.mr)

out_f = open('mapreduce/reduce.%d.out' % args.shard, 'w')
cur_key = None
values = []
for line in open('mapreduce/reduce.%d.in' % args.shard):
    j = json.loads(line)
    key, value = j[u'kv']
    if key == cur_key:
        values.append(value)
    else:
        for v in module.reduce(cur_key, values):
            out_f.write(v + '\n')
        cur_key = key
        values = [value]
out_f.close()

open('mapreduce/reduce.%d.done' % args.shard, 'w').write('')
