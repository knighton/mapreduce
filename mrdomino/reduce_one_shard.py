import imp
import json
import sys
from os.path import join as path_join
from functools import partial
from contextlib import nested as nested_context
from mrdomino import logger
from mrdomino.util import MRCounter


def reduce(shard, args):

    # find the reduce function.
    reduce_module = imp.load_source('reduce_module', args.reduce_module)
    reduce_func = getattr(reduce_module, args.reduce_func)

    # the counters.
    counters = MRCounter()

    # default to work_dir if output_dir is not set
    work_dir = args.work_dir
    output_dir = args.output_dir
    if output_dir is None:
        output_dir = work_dir

    # process each (key, value) pair.
    out_f = path_join(output_dir, args.output_prefix + '.%d' % shard)

    if args.input_prefix is not None:
        # otherwise use input prefix
        in_f = path_join(work_dir, args.input_prefix + '.%d' % shard)
        input_stream = partial(open, in_f, 'r')

    else:
        # otherwise use stdin
        input_stream = sys.stdin

    with nested_context(input_stream(), open(out_f, 'w')) as (in_fh, out_fh):
        last_key = None
        values = []
        for line in in_fh:
            counters.incr("reducer", "seen", 1)
            key, value = json.loads(line)
            if key == last_key:
                # extend previous run
                values.append(value)
            else:
                # end previous run
                if values:
                    for kv in reduce_func(last_key, values, counters.incr):
                        counters.incr("reducer", "written", 1)
                        out_fh.write(json.dumps(kv) + '\n')

                # start new run
                last_key = key
                values = [value]
        # dump any remaining values
        if values:
            for kv in reduce_func(last_key, values, counters.incr):
                counters.incr("reducer", "written", 1)
                out_fh.write(json.dumps(kv) + '\n')

    # write out the counters to file.
    f = path_join(output_dir, 'reduce.counters.%d' % shard)
    logger.info("writing counters to disk at {}".format(f))
    with open(f, 'w') as fh:
        fh.write(counters.serialize())

    # finally note that we are done.
    f = path_join(output_dir, 'reduce.done.%d' % shard)
    with open(f, 'w') as fh:
        fh.write('')
