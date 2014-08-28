import imp
import json
import os
import itertools

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
    in_f = '%s/reduce.in.%d' % (work_dir, shard)
    out_f = '%s/reduce.out.%d' % (output_dir, shard)
    with open(in_f, 'r') as in_fh:
        with open(out_f, 'w') as out_fh:
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
    f = '%s/reduce.counters.%d' % (work_dir, shard)
    with open(f, 'w') as fh:
        fh.write(json_str_from_counters(counters))

    # finally note that we are done.
    f = '%s/reduce.done.%d' % (work_dir, shard)
    with open(f, 'w') as fh:
        fh.write('')
