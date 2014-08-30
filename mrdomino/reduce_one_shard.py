import imp
import json
from os.path import join as path_join
from glob import glob
from functools import partial
from contextlib import nested as nested_context

from mrdomino.util import MRCounter, MRFileInput


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
    lines_seen_counter = "lines seen (step %d)" % args.step_idx
    lines_written_counter = "lines written (step %d)" % args.step_idx

    if args.glob_prefix is None:
        # use input prefix by default
        in_f = path_join(work_dir, args.input_prefix + '.%d' % shard)
        input_stream = partial(open, in_f, 'r')
    else:
        # if glob_prefix is set, use it
        files = glob(path_join(work_dir, args.glob_prefix + '.*'))
        input_stream = partial(MRFileInput, files, 'r')

    with nested_context(input_stream(), open(out_f, 'w')) as (in_fh, out_fh):
        last_key = None
        values = []
        for line in in_fh:
            counters.incr(lines_seen_counter, "reduce", 1)
            key, value = json.loads(line)
            if key == last_key:
                # extend previous run
                values.append(value)
            else:
                # end previous run
                if values:
                    for kv in reduce_func(last_key, values, counters.incr):
                        counters.incr(lines_written_counter, "reduce", 1)
                        out_fh.write(json.dumps(kv) + '\n')

                # start new run
                last_key = key
                values = [value]
        # dump any remaining values
        if values:
            for kv in reduce_func(last_key, values, counters.incr):
                counters.incr(lines_written_counter, "reduce", 1)
                out_fh.write(json.dumps(kv) + '\n')

    # write out the counters to file.
    f = path_join(output_dir, 'reduce.counters.%d' % shard)
    with open(f, 'w') as fh:
        fh.write(counters.to_json())

    # finally note that we are done.
    f = path_join(output_dir, 'reduce.done.%d' % shard)
    with open(f, 'w') as fh:
        fh.write('')
