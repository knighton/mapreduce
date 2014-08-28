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

    if not os.path.exists(args.output_dir):
        try:
            os.makedirs(args.output_dir)
        except:
            pass

    # process each (key, value) pair.
    in_f = os.path.join(args.work_dir, 'reduce.in.%d' % shard)
    out_f = os.path.join(args.output_dir, 'reduce.out.%d' % shard)
    with nested_context(open(in_f, 'r'), open(out_f, 'w')) as (in_fh, out_fh):
        cur_key = None
        values = []
        while True:
            try:
                line = in_fh.next()
            except StopIteration:
                # dump any remaining content
                if cur_key is not None:
                    for kv in reduce_func(cur_key, values, increment_counter):
                        out_fh.write(json.dumps(kv) + '\n')
                break
            key, value = json.loads(line)
            if key == cur_key:
                values.append(value)
            else:
                for kv in reduce_func(cur_key, values, increment_counter):
                    out_fh.write(json.dumps(kv) + '\n')
                cur_key = key
                values = [value]

    # write out the counters to file.
    f = os.path.join(args.work_dir, 'reduce.counters.%d' % shard)
    with open(f, 'w') as fh:
        fh.write(json_str_from_counters(counters))

    # finally note that we are done.
    f = os.path.join(args.work_dir, 'reduce.done.%d' % shard)
    with open(f, 'w') as fh:
        fh.write('')
