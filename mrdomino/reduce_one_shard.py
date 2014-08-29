import imp
import json
import os
from contextlib import nested as nested_context

from mrdomino.util import json_str_from_counters, NestedCounter


def reduce(shard, args):

    # find the reduce function.
    reduce_module = imp.load_source('reduce_module', args.reduce_module)
    reduce_func = getattr(reduce_module, args.reduce_func)

    # the counters.
    counters = NestedCounter()

    def increment_counter(key, sub_key, incr):
        counters[key][sub_key] += incr

    # process each (key, value) pair.
    in_f = os.path.join(args.work_dir, 'reduce.in.%d' % shard)
    out_f = os.path.join(args.output_dir, 'reduce.out.%d' % shard)
    with nested_context(open(in_f, 'r'), open(out_f, 'w')) as (in_fh, out_fh):
        last_key = None
        values = []
        for line in in_fh:
            key, value = json.loads(line)
            if key == last_key:
                # extend previous run
                values.append(value)
            else:
                # end previous run
                if values:
                    for kv in reduce_func(last_key, values, increment_counter):
                        out_fh.write(json.dumps(kv) + '\n')

                # start new run
                last_key = key
                values = [value]
        # dump any remaining values
        if values:
            for kv in reduce_func(last_key, values, increment_counter):
                out_fh.write(json.dumps(kv) + '\n')

    # write out the counters to file.
    f = os.path.join(args.work_dir, 'reduce.counters.%d' % shard)
    with open(f, 'w') as fh:
        fh.write(json_str_from_counters(counters))

    # finally note that we are done.
    f = os.path.join(args.work_dir, 'reduce.done.%d' % shard)
    with open(f, 'w') as fh:
        fh.write('')
