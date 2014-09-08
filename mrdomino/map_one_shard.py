import json
import math
import itertools
from os.path import join as path_join
from subprocess import Popen, PIPE
from mrdomino import logger, get_instance, protocol
from mrdomino.util import create_cmd, open_input


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

    # find the map function.
    job = get_instance(args)
    step = job.get_step(args.step_idx)
    map_func = step.mapper
    n_shards = step.n_mappers
    combine_func = step.combiner

    assert 0 <= shard < n_shards

    if combine_func is None:
        out_fn = path_join(args.work_dir, args.output_prefix + '.%d' % shard)
        logger.info("mapper {}: output -> {}".format(shard, out_fn))
        proc_sort = Popen(['sort', '-o', out_fn], bufsize=4096, stdin=PIPE)
        proc = proc_sort
    else:
        cmd_opts = ['python', '-m', 'mrdomino.combine',
                    '--job_module', args.job_module,
                    '--job_class', args.job_class,
                    '--step_idx', str(args.step_idx),
                    '--work_dir', args.work_dir,
                    '--output_prefix', args.output_prefix,
                    '--shard', str(shard)]
        logger.info("mapper {}: starting combiner: {}"
                    .format(shard, create_cmd(cmd_opts)))
        proc_combine = Popen(cmd_opts, bufsize=4096, stdin=PIPE)
        proc_sort = Popen(['sort'], bufsize=4096, stdin=PIPE,
                          stdout=proc_combine.stdin)
        proc = proc_combine

    if args.step_idx == 0:
        # first step
        if job.INPUT_PROTOCOL == protocol.JSONProtocol:
            unpack_tuple = True
        elif job.INPUT_PROTOCOL == protocol.JSONValueProtocol:
            unpack_tuple = False
        else:
            raise ValueError("unsupported protocol: {}"
                             .format(job.INPUT_PROTOCOL))
    elif args.step_idx > 0:
        # intermediate step
        if job.INTERNAL_PROTOCOL == protocol.JSONProtocol:
            unpack_tuple = True
        elif job.INTERNAL_PROTOCOL == protocol.JSONValueProtocol:
            unpack_tuple = False
        else:
            raise ValueError("unsupported protocol: {}"
                             .format(job.INTERNAL_PROTOCOL))
    else:
        raise ValueError("step_idx={} cannot be negative"
                         .format(args.step_idx))

    # process each line of input and sort for the merge step.
    # using with block here ensures that proc_sort.stdin is closed on exit and
    # that it won't block the pipeline
    count_written = 0
    count_seen = 0
    with proc_sort.stdin as in_fh:
        for line in each_input_line(args.input_files, shard, n_shards):
            count_seen += 1
            kv = json.loads(line)
            k, v = kv if unpack_tuple else (None, kv)
            for kv in map_func(k, v):
                in_fh.write(json.dumps(kv) + '\n')
                count_written += 1

    counters = job._counters
    counters.incr("mapper", "seen", count_seen)
    counters.incr("mapper", "written", count_written)

    # write out the counters to file.
    f = path_join(args.work_dir, 'map.counters.%d' % shard)
    logger.info("mapper {}: counters -> {}".format(shard, f))
    with open(f, 'w') as fh:
        fh.write(counters.serialize())

    # write how many entries were written for reducer balancing purposes.
    # note that if combiner is present, we delegate this responsibility to it.
    if combine_func is None:
        f = path_join(args.work_dir, args.output_prefix + '_count.%d' % shard)
        logger.info("mapper {}: lines written -> {}".format(shard, f))
        with open(f, 'w') as fh:
            fh.write(str(count_written))

    # `communicate' will wait for subprocess to terminate
    comb_stdout, comb_stderr = proc.communicate()

    # finally note that we are done.
    f = path_join(args.work_dir, 'map.done.%d' % shard)
    logger.info("mapper {}: done -> {}".format(shard, f))
    with open(f, 'w') as fh:
        fh.write('')
