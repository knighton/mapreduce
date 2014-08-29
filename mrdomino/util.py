import collections
import json
import os
import time
import random
import string
import functools

NestedCounter = functools.partial(collections.defaultdict, collections.Counter)


class MRCounter(collections.Iterable):
    def __init__(self):
        self.counter = NestedCounter()

    def __iter__(self):
        return self.counter.__iter()

    def iteritems(self):
        return self.counter.iteritems()

    def incr(self, key, sub_key, incr):
        self.counter[key][sub_key] += incr


def create_cmd(prefix, opts=None):
    if opts is None:
        return prefix

    suffix = ' '.join('--{} {}'.format(k, v) for k, v in opts.iteritems())
    return prefix + ' ' + suffix


class Timer(object):
    def __enter__(self):
        self.clock_start = time.clock()
        self.wall_start = time.time()
        return self

    def __exit__(self, *args):
        clock_end = time.clock()
        wall_end = time.time()
        self.clock_interval = clock_end - self.clock_start
        self.wall_interval = wall_end - self.wall_start

    def __str__(self):
        return "clock: %0.03f sec, wall: %0.03f sec." \
            % (self.clock_interval, self.wall_interval)


def json_str_from_counters(counters):
    arr = []
    for key, sub_dict in sorted(counters.iteritems()):
        for sub_key, count in sorted(sub_dict.iteritems()):
            arr.append({
                'key': key,
                'sub_key': sub_key,
                'count': count,
            })
    return json.dumps({
        'counters': arr,
    })


def counters_from_json_str(s):
    r = NestedCounter()
    j = json.loads(s)
    for d in j['counters']:
        key = d['key']
        sub_key = d['sub_key']
        count = d['count']
        r[key][sub_key] += count
    return r


def combine_counters_from_files(ff):
    r = NestedCounter()
    for f in ff:
        with open(f, 'r') as fh:
            s = fh.read()
        d = counters_from_json_str(s)
        for key, val in d.iteritems():
            r[key].update(val)
    return r


def show_counters(d):
    if d:
        print 'Counters:'
    for key in sorted(d):
        print '  %s:' % key
        for sub_key in sorted(d[key]):
            count = d[key][sub_key]
            print '    %s: %d' % (sub_key, count)


def show_combined_counters_from_files(ff):
    d = combine_counters_from_files(ff)
    show_counters(d)


def show_combined_counters(work_dir, n_map_shards, n_reduce_shards):
    ff = map(lambda (work_dir, shard):
             os.path.join(work_dir, 'map.counters.%d' % shard),
             zip([work_dir] * n_map_shards, range(n_map_shards)))
    ff += map(lambda (work_dir, shard):
              os.path.join(work_dir, 'reduce.counters.%d' % shard),
              zip([work_dir] * n_reduce_shards, range(n_reduce_shards)))
    ff = filter(os.path.exists, ff)
    show_combined_counters_from_files(ff)


def random_string(length):
    choices = string.ascii_lowercase + string.ascii_uppercase + string.digits
    cc = []
    for i in range(length):
        cc.append(random.choice(choices))
    return ''.join(cc)
