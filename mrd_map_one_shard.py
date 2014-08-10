#!/usr/bin/python

import collections
import imp
import json
import math
import os

import mrd_util


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


def map(map_module, map_func, input_files, work_dir, shard, n_shards):
    assert 0 <= shard < n_shards

    # find the map function.
    map_module = imp.load_source('map_module', map_module)
    map_func = getattr(map_module, map_func)

    # the counters.
    counters = collections.defaultdict(lambda: collections.defaultdict(int))

    def increment_counter(key, sub_key, incr):
        counters[key][sub_key] += incr

    # process each line of input.
    out_fn = '%s/map.out.%d' % (work_dir, shard)
    out_f = open(out_fn, 'w')
    count = 0
    for line in each_input_line(input_files, shard, n_shards):
        for key, value in map_func(line, increment_counter):
            j = {'kv': [key, value]}
            out_f.write(json.dumps(j) + '\n')
            count += 1
    out_f.close()

    # write out the counters to file.
    f = '%s/map.counters.%d' % (work_dir, shard)
    open(f, 'w').write(mrd_util.json_str_from_counters(counters))

    # write how many entries were written for reducer balancing purposes.
    f = '%s/map.out_count.%d' % (work_dir, shard)
    open(f, 'w').write(str(count))

    # sort the results for merge step.
    os.system('sort %s -o %s' % (out_fn, out_fn))

    # finally note that we are done.
    open('%s/map.done.%d' % (work_dir, shard), 'w').write('')
