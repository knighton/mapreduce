import re
import glob
import json
from mrdomino import mapreduce, MRStep, MRSettings


def get_tld(domain):
    return re.match(r'^.*\b([^\.]+\.[^\.]+)$', domain).group(1)


def map1(_, line, increment_counter):
    j = json.loads(line)
    key = j[u'object'][u'user_id']
    uname, domain = key.split("@")
    tld = get_tld(domain)
    increment_counter("TLD map1", tld, 1)
    yield key, 1


def combine1(key, vals, increment_counter):
    total = sum(vals)
    yield key, total


def reduce1(key, vals, increment_counter):
    total = sum(vals)
    uname, domain = key.split("@")
    tld = get_tld(domain)
    increment_counter("TLD reduce1", tld, total)
    yield key, total    # username -> count of posts


def combine2(key, vals, increment_counter):
    total = sum(vals)
    yield key, total


def map2(key, val, increment_counter):
    uname, domain = key.split("@")
    tld = get_tld(domain)
    increment_counter("TLD map2", tld, val)
    yield domain, val


def reduce2(key, vals, increment_counter):
    total = sum(vals)
    tld = get_tld(key)
    increment_counter("TLD reduce2", tld, total)
    yield key, total


def main():
    steps = [
        MRStep(
            mapper=map1,
            combiner=combine1,
            reducer=reduce1,
            n_mappers=2,
            n_reducers=3
        ),
        MRStep(
            mapper=map2,
            combiner=combine2,
            reducer=reduce2,
            n_mappers=3,
            n_reducers=2
        )
    ]
    settings = MRSettings(
        input_files=glob.glob('./data/2014-01-18.detail.10000'),
        output_dir='out',
        tmp_dir='tmp',
        use_domino=False,
        n_concurrent_machines=2,
        n_shards_per_machine=3
    )
    mapreduce(steps, settings)


if __name__ == '__main__':
    main()
