#!/usr/bin/python

import json


def map(s):
    j = json.loads(s)
    yield j[u'object'][u'user_id'], j


def reduce(k, vv):
    yield '%s -> %d posts' % (k, len(vv))
