#!/usr/bin/python

import collections
import json
import os
import random
import string


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
