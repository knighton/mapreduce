import re
import glob
import json
from mrdomino import mapreduce


def map1(_, line, increment_counter):
    j = json.loads(line)
    key = j[u'object'][u'user_id']
    uname, domain = key.split("@")
    tld = re.match(r'^.*\b([^\.]+\.[^\.]+)$', domain).group(1)
    increment_counter("TLD map1", tld, 1)
    yield key, 1


def reduce1(key, vals, increment_counter):
    total = sum(vals)
    uname, domain = key.split("@")
    tld = re.match(r'^.*\b([^\.]+\.[^\.]+)$', domain).group(1)
    increment_counter("TLD reduce1", tld, total)
    yield key, total    # username -> count of posts


def map2(key, val, increment_counter):
    uname, domain = key.split("@")
    tld = re.match(r'^.*\b([^\.]+\.[^\.]+)$', domain).group(1)
    increment_counter("TLD map2", tld, val)
    yield domain, val


def reduce2(key, vals, increment_counter):
    total = sum(vals)
    tld = re.match(r'^.*\b([^\.]+\.[^\.]+)$', key).group(1)
    increment_counter("TLD reduce2", tld, total)
    yield key, total


def main():
    steps = [
        {
            'mapper': map1,
            'n_mappers': 2,
            'reducer': reduce1,
            'n_reducers': 3,
        },
        {
            'mapper': map2,
            'n_mappers': 5,
            'reducer': reduce2,
            'n_reducers': 4,
        }
    ]

    settings = {
        'use_domino': False,
        'n_concurrent_machines': 2,
        'n_shards_per_machine': 3,
        'input_files': glob.glob('./data/2014-01-18.detail.10000'),
        'output_dir': 'out',
        'tmp_dir': 'tmp'
    }

    mapreduce(steps, settings)


if __name__ == '__main__':
    main()
