#!/usr/bin/python

from argparse import ArgumentParser
import collections
import imp
import json
import os

import mrd_util

ap = ArgumentParser()
ap.add_argument('--shard', type=int,
                help='which shard we are')
ap.add_argument('--n_shards', type=int,
                help='out of how many shards')
ap.add_argument('--reduce_module', type=str,
                help='path to module containing reducer')
ap.add_argument('--reduce_func', type=str,
                help='reduce function name')
ap.add_argument('--work_dir', type=str,
                help='directory containing reduce input files')
ap.add_argument('--output_dir', type=str, default='.',
                help='directory containing reduce output files')
args = ap.parse_args()

assert 0 <= args.shard < args.n_shards
assert args.work_dir


# find the reduce function.
reduce_module = imp.load_source('reduce_module', args.reduce_module)
reduce_func = getattr(reduce_module, args.reduce_func)


# the counters.
counters = collections.defaultdict(lambda: collections.defaultdict(int))
def increment_counter(key, sub_key, incr):
    counters[key][sub_key] += incr


if not os.path.exists(args.output_dir):
    os.makedirs(args.output_dir)


# process each (key, value) pair.
out_f = open('%s/reduce.out.%d' % (args.output_dir, args.shard), 'w')
cur_key = None
values = []
for line in open('%s/reduce.in.%d' % (args.work_dir, args.shard)):
    j = json.loads(line)
    key, value = j[u'kv']
    if key == cur_key:
        values.append(value)
    else:
        for v in reduce_func(cur_key, values, increment_counter):
            out_f.write(v + '\n')
        cur_key = key
        values = [value]
out_f.close()


# write out the counters to file.
f = '%s/reduce.counters.%d' % (args.work_dir, args.shard)
open(f, 'w').write(mrd_util.json_str_from_counters(counters))


# finally note that we are done.
open('%s/reduce.done.%d' % (args.work_dir, args.shard), 'w').write('')
