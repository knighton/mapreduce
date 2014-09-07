import json
import sys
from os.path import join as path_join
from functools import partial
from contextlib import nested as nested_context
from mrdomino import logger, get_instance


def reduce(shard, args):

    # find the reduce function.
    job = get_instance(args)
    step = job.get_step(args.step_idx)
    reduce_func = step.reducer

    # default to work_dir if output_dir is not set
    work_dir = args.work_dir
    output_dir = args.output_dir
    if output_dir is None:
        output_dir = work_dir

    # process each (key, value) pair.
    out_fn = path_join(output_dir, args.output_prefix + '.%d' % shard)
    logger.info("reducer output -> {}".format(out_fn))

    if args.input_prefix is not None:
        # otherwise use input prefix
        in_f = path_join(work_dir, args.input_prefix + '.%d' % shard)
        input_stream = partial(open, in_f, 'r')

    else:
        # otherwise use stdin
        input_stream = sys.stdin

    count_written = 0
    count_seen = 0
    with nested_context(input_stream(), open(out_fn, 'w')) as (in_fh, out_fh):
        last_key = None
        values = []
        for line in in_fh:
            count_seen += 1
            key, value = json.loads(line)
            if key == last_key:
                # extend previous run
                values.append(value)
            else:
                # end previous run
                if values:
                    for kv in reduce_func(last_key, values):
                        count_written += 1
                        out_fh.write(json.dumps(kv) + '\n')

                # start new run
                last_key = key
                values = [value]
        # dump any remaining values
        if values:
            for kv in reduce_func(last_key, values):
                count_written += 1
                out_fh.write(json.dumps(kv) + '\n')

    counters = job._counters
    counters.incr("reducer", "seen", count_seen)
    counters.incr("reducer", "written", count_written)

    # write out the counters to file.
    f = path_join(output_dir, 'reduce.counters.%d' % shard)
    logger.info("reducer counters -> {}".format(f))
    with open(f, 'w') as fh:
        fh.write(counters.serialize())

    # finally note that we are done.
    f = path_join(output_dir, 'reduce.done.%d' % shard)
    with open(f, 'w') as fh:
        fh.write('')
