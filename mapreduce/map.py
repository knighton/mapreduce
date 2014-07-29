#!/usr/bin/python

from argparse import ArgumentParser
import imp
import json
import os

ap = ArgumentParser()
ap.add_argument('--shard', type=int,
                help='which shard we are')
ap.add_argument('--n_shards', type=int,
                help='out of how many shards')
ap.add_argument('--input_files', type=str, nargs='+',
                help='input files')
ap.add_argument('--mr', type=str,
                help='path to module containing map() and reduce() to use')
ap.add_argument('--work_dir', type=str,
                help='directory containing map output files')
args = ap.parse_args()

assert 0 <= args.shard < args.n_shards
assert args.work_dir

module = imp.load_source('module', args.mr)

# get our slice of the input files.
ff = args.input_files
assert args.n_shards <= len(ff)
a = len(ff) * args.shard / args.n_shards
z = len(ff) * (args.shard + 1) / args.n_shards
ff = ff[a:z]

# process each file.
out_fn = '%s/map.out.%d' % (args.work_dir, args.shard)
out_f = open(out_fn, 'w')
count = 0
for f in ff:
    for line in open(f):
        for key, value in module.map(line):
            j = {'kv': [key, value]}
            out_f.write(json.dumps(j) + '\n')
            count += 1
out_f.close()

f = '%s/map.out_count.%d' % (args.work_dir, args.shard)
open(f, 'w').write(str(count))

# sort the results for merge step.
os.system('sort %s -o %s' % (out_fn, out_fn))

# finally note that we are done.
open('%s/map.done.%d' % (args.work_dir, args.shard), 'w').write('')
