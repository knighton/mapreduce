#!/usr/bin/python

from argparse import ArgumentParser
import imp
import json
import math
import os

ap = ArgumentParser()
ap.add_argument('--shard', type=int,
                help='which shard we are')
ap.add_argument('--n_shards', type=int,
                help='out of how many shards')
ap.add_argument('--input_files', type=str, nargs='+',
                help='input files')
ap.add_argument('--map_module', type=str,
                help='path to module containing mapper')
ap.add_argument('--map_func', type=str,
                help='mapper function name')
ap.add_argument('--work_dir', type=str,
                help='directory containing map output files')
args = ap.parse_args()

assert 0 <= args.shard < args.n_shards
assert args.work_dir


def each_input_line(input_files, shard, n_shards):
    # assign slices of each file to shards.
    slice_assignments = []
    for i in range(n_shards):
        slice_assignments += [i] * len(input_files)

    # get which files this shard is using (partially or the whole file).
    a = len(input_files) * shard / n_shards
    z = len(input_files) * (shard + 1) / float(n_shards)
    z = int(math.ceil(z))

    # for each input file, yield the slices we want from it.
    for i in range(a, z):
        aa = n_shards * i
        zz = n_shards * (i + 1)
        assign = slice_assignments[aa:zz]
        f = input_files[i]
        f = open(f)
        done = False
        while not done:
            for j in range(n_shards):
                s = f.readline()
                if not s:
                    done = True
                    break
                if assign[j] == shard:
                    yield s


# find the map function.
map_module = imp.load_source('map_module', args.map_module)
map_func = getattr(map_module, args.map_func)


# process each line of input.
out_fn = '%s/map.out.%d' % (args.work_dir, args.shard)
out_f = open(out_fn, 'w')
count = 0
for line in each_input_line(args.input_files, args.shard, args.n_shards):
    for key, value in map_func(line):
        j = {'kv': [key, value]}
        out_f.write(json.dumps(j) + '\n')
        count += 1
out_f.close()


# write how many entries were written for reducer balancing purposes.
f = '%s/map.out_count.%d' % (args.work_dir, args.shard)
open(f, 'w').write(str(count))


# sort the results for merge step.
os.system('sort %s -o %s' % (out_fn, out_fn))


# finally note that we are done.
open('%s/map.done.%d' % (args.work_dir, args.shard), 'w').write('')