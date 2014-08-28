import collections
import json
import os
import time
import random
import string


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
    jj = []
    for key in sorted(counters):
        for sub_key in sorted(counters[key]):
            count = counters[key][sub_key]
            j = {
                'key': key,
                'sub_key': sub_key,
                'count': count,
            }
            jj.append(j)
    d = {
        'counters': jj,
    }
    return json.dumps(d)


def counters_from_json_str(s):
    r = collections.defaultdict(lambda: collections.defaultdict(int))
    j = json.loads(s)
    for d in j['counters']:
        key = d['key']
        sub_key = d['sub_key']
        count = d['count']
        r[key][sub_key] += count
    return r


def combine_counters_from_files(ff):
    r = collections.defaultdict(lambda: collections.defaultdict(int))
    for f in ff:
        s = open(f).read()
        d = counters_from_json_str(s)
        for key in d:
            for sub_key in d[key]:
                r[key][sub_key] += d[key][sub_key]
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
    ff = map(lambda (work_dir, shard): '%s/map.counters.%d' % (work_dir, shard),
             zip([work_dir] * n_map_shards,
                 range(n_map_shards)))
    ff += map(lambda (work_dir, shard):
              '%s/reduce.counters.%d' % (work_dir, shard),
              zip([work_dir] * n_reduce_shards,
                  range(n_reduce_shards)))
    ff = filter(os.path.exists, ff)
    show_combined_counters_from_files(ff)


def random_string(length):
    choices = string.ascii_lowercase + string.ascii_uppercase + string.digits
    cc = []
    for i in range(length):
        cc.append(random.choice(choices))
    return ''.join(cc)


def mk_tmpdir():
    return 'tmp/%s' % random_string(16)
