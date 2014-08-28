import sys
import os
import subprocess
from mrdomino import util
from pkg_resources import resource_filename
from tempfile import mkdtemp

EXEC_SCRIPT = resource_filename(__name__, "exec.sh")


def mapreduce(steps, settings):

    step_count = len(steps)

    # if temporary directory root does not exist, create one
    tmp_root = settings['tmp_dir']
    if not os.path.exists(tmp_root):
        os.makedirs(tmp_root)
    tmp_dirs = [mkdtemp(dir=tmp_root) for _ in range(step_count - 1)]

    input_file_lists = [settings['input_files']]
    output_dirs = []
    for step, out_dir in zip(steps, tmp_dirs):
        n_reducers = step['n_reducers']
        reduce_format = os.path.join(out_dir, 'reduce.out.%d')
        ff = [reduce_format % n for n in range(n_reducers)]
        input_file_lists.append(ff)
        output_dirs.append(out_dir)

    # if output directory root does not exist, create one
    output_dir = settings['output_dir']
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_dirs.append(output_dir)

    for i, step in enumerate(steps):
        cmd = util.create_cmd(EXEC_SCRIPT + ' mrdomino.step', {
            'step_idx': i,
            'total_steps': step_count,
            'input_files': ' '.join(input_file_lists[i]),
            'output_dir': output_dirs[i],
            'work_dir': mkdtemp(dir=tmp_root),
            'map_module': step['mapper'].func_globals['__file__'],
            'map_func': step['mapper'].func_name,
            'n_map_shards': step['n_mappers'],
            'reduce_module': step['reducer'].func_globals['__file__'],
            'reduce_func': step['reducer'].func_name,
            'n_reduce_shards': step['n_reducers'],
            'use_domino': int(settings['use_domino']),
            'n_concurrent_machines': settings['n_concurrent_machines'],
            'n_shards_per_machine': settings['n_shards_per_machine']
        })
        try:
            with util.Timer() as t:
                retcode = subprocess.call(cmd, shell=True)
            if retcode < 0:
                print >>sys.stderr, \
                    "Step {} terminated by signal {}".format(i, -retcode)
            else:
                print >>sys.stderr, \
                    "Step {} finished with status code {}".format(i, retcode)
        except OSError as e:
            print >>sys.stderr, "Step {} failed: {}".format(i, e)
        print >>sys.stderr, "Timer stats: {}".format(str(t))
    print 'All done.'
