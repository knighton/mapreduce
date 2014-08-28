import glob
import json
from mrdomino import mapreduce


def map1(_, line, increment_counter):
    j = json.loads(line)
    yield j[u'object'][u'user_id'], 1


def reduce1(key, vals, increment_counter):

    # username -> count of posts
    yield key, sum(vals)


def map2(key, val, increment_counter):
    # find domains
    if key is None:
        increment_counter('map2', 'key_is_None', 1)
    else:
        t = key.split("@")
        if len(t) == 2:
            yield t[1], val
        else:
            increment_counter('map2', 'no_domain', 1)


def reduce2(key, vals, increment_counter):
    yield key, sum(vals)


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
    }

    mapreduce(steps, settings)


if __name__ == '__main__':
    main()
