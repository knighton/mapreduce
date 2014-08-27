import collections
import imp
import json
import os

from mrdomino.util import json_str_from_counters


def reduce(reduce_module, reduce_func, work_dir, output_dir, shard):
    # find the reduce function.
    reduce_module = imp.load_source('reduce_module', reduce_module)
    reduce_func = getattr(reduce_module, reduce_func)

    # the counters.
    counters = collections.defaultdict(lambda: collections.defaultdict(int))

    def increment_counter(key, sub_key, incr):
        counters[key][sub_key] += incr

    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
        except:
            pass

    # process each (key, value) pair.
    out_f = open('%s/reduce.out.%d' % (output_dir, shard), 'w')
    cur_key = None
    values = []
    for line in open('%s/reduce.in.%d' % (work_dir, shard)):
        j = json.loads(line)
        key, value = j[u'kv']
        if key == cur_key:
            values.append(value)
        else:
            for v in reduce_func(cur_key, values, increment_counter):
                out_f.write(v + '\n')
            cur_key = key
            values = [value]
    out_f.close()

    # write out the counters to file.
    f = '%s/reduce.counters.%d' % (work_dir, shard)
    open(f, 'w').write(json_str_from_counters(counters))

    # finally note that we are done.
    open('%s/reduce.done.%d' % (work_dir, shard), 'w').write('')
