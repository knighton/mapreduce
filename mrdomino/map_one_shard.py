import imp
import json
import math
import os
import itertools

from mrdomino.util import json_str_from_counters, MRCounter


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
    inf_gen = itertools.cycle(range(n_shards))
    for i in range(a, z):
        aa = n_shards * i
        zz = n_shards * (i + 1)
        assign = slice_assignments[aa:zz]
        with open(input_files[i], 'r') as fh:
            for j, line in itertools.izip(inf_gen, fh):
                if shard == assign[j]:
                    yield line


def map(shard, args):
    assert 0 <= shard < args.n_shards

    # find the map function.
    map_module = imp.load_source('map_module', args.map_module)
    map_func = getattr(map_module, args.map_func)

    # the counters.
    counters = MRCounter()

    # process each line of input.
    count = 0
    out_fn = os.path.join(args.work_dir, 'map.out.%d' % shard)
    unpack_tuple = args.step_idx > 0
    with open(out_fn, 'w') as out_f:
        for line in each_input_line(args.input_files, shard, args.n_shards):
            k, v = json.loads(line) if unpack_tuple else (None, line)
            for kv in map_func(k, v, counters.incr):
                out_f.write(json.dumps(kv) + '\n')
                count += 1

    # write out the counters to file.
    f = os.path.join(args.work_dir, 'map.counters.%d' % shard)
    with open(f, 'w') as fh:
        fh.write(json_str_from_counters(counters))

    # write how many entries were written for reducer balancing purposes.
    f = os.path.join(args.work_dir, 'map.out_count.%d' % shard)
    with open(f, 'w') as fh:
        fh.write(str(count))

    # sort the results for merge step.
    os.system('sort %s -o %s' % (out_fn, out_fn))

    # finally note that we are done.
    f = os.path.join(args.work_dir, 'map.done.%d' % shard)
    with open(f, 'w') as fh:
        fh.write('')