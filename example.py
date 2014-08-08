#!/usr/bin/python

import glob
import json

import mrdomino


def map1(line, increment_counter):
    j = json.loads(line)
    yield j[u'object'][u'user_id'], j


def reduce1(k, vv, increment_counter):
    yield '%s -> %d posts' % (k, len(vv))


def map2(line, increment_counter):
    ss = line.split()
    for s in ss:
        increment_counter('map2', 'len_%03d' % len(s), 10)
        yield s, 1


def reduce2(k, vv, increment_counter):
    yield '%s -> %d' % (k, sum(map(int, vv)))


def main():
    steps = [
        {
            'mapper': map1,
            'n_mappers': 4,
            'reducer': reduce1,
            'n_reducers': 5,
        },
        {
            'mapper': map2,
            'n_mappers': 10,
            'reducer': reduce2,
            'n_reducers': 7,
        }
    ]

    settings = {
        'use_domino': False,
        'n_concurrent_machines': 2,
        'n_shards_per_machine': 3,
        'input_files': glob.glob('data/short.*'),
        'output_dir': 'out',
    }

    mrdomino.mapreduce(steps, settings)


if __name__ == '__main__':
    main()
