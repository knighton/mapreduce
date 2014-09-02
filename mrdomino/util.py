import collections
import json
import time
import subprocess
import operator
import functools
import fileinput


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
        """Add another counter (allows += operator)

        >>> c = MRCounter()
        >>> c.incr("a", "b", 2)
        >>> c.incr("b", "c", 3)
        >>> d = MRCounter()
        >>> d.incr("c", "b", 2)
        >>> d.incr("b", "c", 30)
        >>> d += c
        >>> d.counter['b']['c']
        33

        """
        counter = self.counter
        for key, val in d.iteritems():
            counter[key].update(val)
        return self

    def show(self):
        output = ['Counters:']
        for key, sub_dict in sorted(self.iteritems()):
            output.append('  %s:' % key)
            for sub_key, count in sorted(sub_dict.iteritems()):
                output.append('    %s: %d' % (sub_key, count))
        return '\n'.join(output)

    def serialize(self):
        """
        >>> c = MRCounter()
        >>> c.incr("a", "b", 2)
        >>> c.incr("b", "c", 3)
        >>> type(c.serialize())
        <type 'str'>

        """
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
    def deserialize(cls, s):
        """

        >>> c = MRCounter()
        >>> c.incr("a", "b", 2)
        >>> c.incr("b", "c", 3)
        >>> d = MRCounter.deserialize(c.serialize())
        >>> c.counter == d.counter
        True

        """
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
        """Sum a series of instances of cls

        >>> c = MRCounter()
        >>> c.incr("a", "b", 2)
        >>> c.incr("b", "c", 3)
        >>> d = MRCounter()
        >>> d.incr("c", "b", 2)
        >>> d.incr("b", "c", 30)
        >>> e = MRCounter.sum([c, d])
        >>> e.counter['b']['c']
        33

        """
        return reduce(operator.__iadd__, iterable, cls())


class MRTimer(object):
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


class MRFileInput(object):
    """Emulates context behavior of fileinput in Python 3"""

    def __init__(self, files, mode='r'):
        self.files = files
        self.mode = mode

    def __enter__(self):
        self.fh = fileinput.input(self.files, mode=self.mode)
        return self.fh

    def __exit__(self, *args):
        self.fh.close()


def wait_cmd(cmd, logger, name="Command"):
    try:
        with MRTimer() as t:
            retcode = subprocess.call(cmd, shell=True)
        if retcode < 0:
            logger.error("{} terminated by signal {}".format(name, -retcode))
        else:
            logger.info(
                "{} finished with status code {}".format(name, retcode))
        logger.info("{} run stats: {}".format(name, str(t)))
    except OSError as e:
        logger.error("{} failed: {}".format(name, e))
    return retcode


def create_cmd(parts):

    sane_parts = []
    for part in parts:
        if not isinstance(part, str):
            part = str(part)
        sane_parts.append(part)

    return ' '.join(sane_parts)


def read_files(filenames):
    for f in filenames:
        with open(f, 'r') as fh:
            yield fh.read()


if __name__ == "__main__":
    import doctest
    doctest.testmod()
