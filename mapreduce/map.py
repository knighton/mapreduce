#!/usr/bin/python

from argparse import ArgumentParser
import imp
import json

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

ff = args.input_files
assert args.n_shards <= len(ff)
a = len(ff) * args.shard / args.n_shards
z = len(ff) * (args.shard + 1) / args.n_shards
ff = ff[a:z]

out_f = open('%s/map.out.%d' % (args.work_dir, args.shard), 'w')
for f in ff:
    for line in open(f):
        for key, value in module.map(line):
            j = {'kv': [key, value]}
            out_f.write(json.dumps(j) + '\n')
out_f.close()

open('%s/map.done.%d' % (args.work_dir, args.shard), 'w').write('')
