import imp
import json
import os
from contextlib import nested as nested_context

from mrdomino.util import MRCounter


def reduce(shard, args):

    # find the reduce function.
    reduce_module = imp.load_source('reduce_module', args.reduce_module)
    reduce_func = getattr(reduce_module, args.reduce_func)

    # the counters.
    counters = MRCounter()

    # process each (key, value) pair.
    in_f = os.path.join(args.work_dir, 'reduce.in.%d' % shard)
    out_f = os.path.join(args.output_dir, 'reduce.out.%d' % shard)
    lines_seen_counter = "lines seen (step %d)" % args.step_idx
    lines_written_counter = "lines written (step %d)" % args.step_idx
    with nested_context(open(in_f, 'r'), open(out_f, 'w')) as (in_fh, out_fh):
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
    f = os.path.join(args.work_dir, 'reduce.counters.%d' % shard)
    with open(f, 'w') as fh:
        fh.write(counters.to_json())

    # finally note that we are done.
    f = os.path.join(args.work_dir, 'reduce.done.%d' % shard)
    with open(f, 'w') as fh:
        fh.write('')
