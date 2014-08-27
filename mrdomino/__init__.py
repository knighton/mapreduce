import sys
import subprocess
from mrdomino import util


def mapreduce(steps, settings):
    tmp_dirs = map(lambda _: util.mk_tmpdir(),
                   range(len(steps) - 1))

    input_file_lists = [settings['input_files']]
    output_dirs = []
    for step, out_dir in zip(steps, tmp_dirs):
        n_reducers = step['n_reducers']
        each_reducer = range(n_reducers)
        ff = map(lambda n: '%s/reduce.out.%d' % (out_dir, n), each_reducer)
        input_file_lists.append(ff)
        output_dirs.append(out_dir)
    output_dirs.append(settings['output_dir'])

    for i, step in enumerate(steps):
        cmd = """python -m mrdomino.step \
    --input_files %s \
    --output_dir %s \
    --map_module %s \
    --map_func %s \
    --n_map_shards %d \
    --reduce_module %s \
    --reduce_func %s \
    --n_reduce_shards %d \
    --use_domino %d \
    --n_concurrent_machines %d \
    --n_shards_per_machine %d
""" % (
            ' '.join(input_file_lists[i]),
            output_dirs[i],
            step['mapper'].func_globals['__file__'],
            step['mapper'].func_name,
            step['n_mappers'],
            step['reducer'].func_globals['__file__'],
            step['reducer'].func_name,
            step['n_reducers'],
            settings['use_domino'],
            settings['n_concurrent_machines'],
            settings['n_shards_per_machine'],
        )
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
