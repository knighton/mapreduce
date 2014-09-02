import imp
import json
import math
import itertools
from os.path import join as path_join
from subprocess import Popen, PIPE
from mrdomino import logger
from mrdomino.util import MRCounter, create_cmd, open_input


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
        inf_gen = itertools.cycle(range(n_shards))
        with open_input(input_files[i], 'r') as fh:
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

    if args.combine_func is None:
        out_fn = path_join(args.work_dir, args.output_prefix + '.%d' % shard)
        logger.info("mapper output -> {}".format(out_fn))
        proc_sort = Popen(['sort', '-o', out_fn], bufsize=4096, stdin=PIPE)
        proc = proc_sort
    else:
        cmd_opts = ['python', '-m', 'mrdomino.combine',
                    '--combine_module', args.combine_module,
                    '--combine_func', args.combine_func,
                    '--work_dir', args.work_dir,
                    '--output_prefix', args.output_prefix,
                    '--shard', str(shard)]
        logger.info("Starting combiner: {}".format(create_cmd(cmd_opts)))
        proc_combine = Popen(cmd_opts, bufsize=4096, stdin=PIPE)
        proc_sort = Popen(['sort'], bufsize=4096, stdin=PIPE,
                          stdout=proc_combine.stdin)
        proc = proc_combine

    unpack_tuple = args.step_idx > 0

    # process each line of input and sort for the merge step.
    # using with block here ensures that proc_sort.stdin is closed on exit and
    # that it won't block the pipeline
    count_written = 0
    count_seen = 0
    with proc_sort.stdin as in_fh:
        for line in each_input_line(args.input_files, shard, args.n_shards):
            count_seen += 1
            k, v = json.loads(line) if unpack_tuple else (None, line)
            for kv in map_func(k, v, counters.incr):
                in_fh.write(json.dumps(kv) + '\n')
                count_written += 1

    counters.incr("mapper", "seen", count_seen)
    counters.incr("mapper", "written", count_written)

    # write out the counters to file.
    f = path_join(args.work_dir, 'map.counters.%d' % shard)
    logger.info("mapper counters -> {}".format(f))
    with open(f, 'w') as fh:
        fh.write(counters.serialize())

    # write how many entries were written for reducer balancing purposes.
    # note that if combiner is present, we delegate this responsibility to it.
    if args.combine_func is not None:
        f = path_join(args.work_dir, args.output_prefix + '_count.%d' % shard)
        logger.info("mapper lines written -> {}".format(f))
        with open(f, 'w') as fh:
            fh.write(str(count_written))

    # `communicate' will wait for subprocess to terminate
    comb_stdout, comb_stderr = proc.communicate()

    # finally note that we are done.
    f = path_join(args.work_dir, 'map.done.%d' % shard)
    with open(f, 'w') as fh:
        fh.write('')
