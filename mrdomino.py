#!/bin/sh

import os

import mrd_util


def mapreduce(steps, settings):
    tmp_dirs = map(lambda _: mrd_util.mk_tmpdir(),
                   range(len(steps) - 1))

    input_file_lists = [settings['input_files']]
    output_dirs = []
    for step, out_dir in zip(steps, tmp_dirs):
        n_reducers = step['n_reducers']
        ff = map(lambda n: '%s/reduce.out.%d' % (out_dir, n), range(n_reducers))
        input_file_lists.append(ff)
        output_dirs.append(out_dir)
    output_dirs.append(settings['output_dir'])

    for i, step in enumerate(steps):
        cmd = """
time python mrd_step.py \
    --input_files %s \
    --output_dir %s \
    --map_module %s \
    --map_func %s \
    --n_map_shards %d \
    --reduce_module %s \
    --reduce_func %s \
    --n_reduce_shards %d \
    --n_concurrent_jobs %d \
    --use_domino %d
""" % (
            ' '.join(input_file_lists[i]),
            output_dirs[i],
            step['mapper'].func_globals['__file__'],
            step['mapper'].func_name,
            step['n_mappers'],
            step['reducer'].func_globals['__file__'],
            step['reducer'].func_name,
            step['n_reducers'],
            settings['n_concurrent_jobs'],
            settings['use_domino'],
        )
        os.system(cmd)
    print 'All done.'
