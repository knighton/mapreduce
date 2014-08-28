import imp
import json
import os
import itertools
from contextlib import nested as nested_context

from mrdomino.util import json_str_from_counters, NestedCounter


def reduce(reduce_module, reduce_func, work_dir, output_dir, shard):

    reduce_module = imp.load_source('reduce_module', reduce_module)
    reduce_func = getattr(reduce_module, reduce_func)
    counters = NestedCounter()

    def increment_counter(key, sub_key, incr):
        counters[key][sub_key] += incr

    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
        except:
            pass

    # process each (key, value) pair.
    cur_key = None
    values = []
    in_f = os.path.join(work_dir, 'reduce.in.%d' % shard)
    out_f = os.path.join(output_dir, 'reduce.out.%d' % shard)
    with nested_context(open(in_f, 'r'), open(out_f, 'w')) as (in_fh, out_fh):
        for j in itertools.imap(json.loads, in_fh):
            key, value = j[u'kv']
            if key == cur_key:
                values.append(value)
            else:
                for v in reduce_func(cur_key, values, increment_counter):
                    out_fh.write(v + '\n')
                cur_key = key
                values = [value]

    # write out the counters to file.
    f = os.path.join(work_dir, 'reduce.counters.%d' % shard)
    with open(f, 'w') as fh:
        fh.write(json_str_from_counters(counters))

    # finally note that we are done.
    f = os.path.join(work_dir, 'reduce.done.%d' % shard)
    with open(f, 'w') as fh:
        fh.write('')
