import collections
import json
import time
import operator
import functools

NestedCounter = functools.partial(collections.defaultdict, collections.Counter)


class MRCounter(collections.Iterable):
    def __init__(self):
        self.counter = NestedCounter()

    def __iter__(self):
        return self.counter.__iter__()

    def iteritems(self):
        return self.counter.iteritems()

    def incr(self, key, sub_key, incr):
        self.counter[key][sub_key] += incr

    def __iadd__(self, d):
        """Add another counter (allows += operator)"""
        counter = self.counter
        for key, val in d.iteritems():
            counter[key].update(val)
        return self

    def show(self):
        print 'Counters:'
        for key, sub_dict in sorted(self.iteritems()):
            print '  %s:' % key
            for sub_key, count in sorted(sub_dict.iteritems()):
                print '    %s: %d' % (sub_key, count)

    def to_json(self):
        arr = []
        for key, sub_dict in sorted(self.iteritems()):
            for sub_key, count in sorted(sub_dict.iteritems()):
                arr.append({
                    'key': key,
                    'sub_key': sub_key,
                    'count': count,
                })
        return json.dumps({
            'counters': arr,
        })

    @classmethod
    def from_json(cls, s):
        r = MRCounter()
        j = json.loads(s)
        for d in j['counters']:
            key = d['key']
            sub_key = d['sub_key']
            count = d['count']
            r.incr(key, sub_key, count)
        return r

    @classmethod
    def sum(cls, iterable):
        """Sum a series of instances of cls"""
        return reduce(operator.__iadd__, iterable, cls())


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


def create_cmd(prefix, opts=None):
    if opts is None:
        return prefix

    suffix = ' '.join('--{} {}'.format(k, v) for k, v in opts.iteritems())
    return prefix + ' ' + suffix


def read_files(filenames):
    for f in filenames:
        with open(f, 'r') as fh:
            yield fh.read()
