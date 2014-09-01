import os
import sys
import logging
from pkg_resources import resource_filename
from tempfile import mkdtemp
from mrdomino import util

logger = logging.getLogger('mrdomino')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


EXEC_SCRIPT = resource_filename(__name__, "exec.sh")


def mapreduce(steps, settings):

    step_count = len(steps)

    # if temporary directory root does not exist, create one
    tmp_root = settings['tmp_dir']
    if not os.path.exists(tmp_root):
        os.makedirs(tmp_root)
    tmp_dirs = [mkdtemp(dir=tmp_root, prefix="step%d." % i)
                for i in range(step_count)]

    input_file_lists = [settings['input_files']]
    for step, out_dir in zip(steps, tmp_dirs):
        n_reducers = step['n_reducers']
        reduce_format = os.path.join(out_dir, 'reduce.out.%d')
        ff = [reduce_format % n for n in range(n_reducers)]
        input_file_lists.append(ff)

    # if output directory root does not exist, create one
    output_dir = settings['output_dir']
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for i, step in enumerate(steps):
        cmd_opts = [
            EXEC_SCRIPT, 'mrdomino.step',
            '--step_idx', i,
            '--total_steps', step_count,
            '--input_files', ' '.join(input_file_lists[i]),
            '--work_dir', tmp_dirs[i],
            '--output_dir', output_dir,
            '--map_module', step['mapper'].func_globals['__file__'],
            '--map_func', step['mapper'].func_name,
            '--n_map_shards', step['n_mappers'],
            '--reduce_module', step['reducer'].func_globals['__file__'],
            '--reduce_func', step['reducer'].func_name,
            '--n_reduce_shards', step['n_reducers'],
            '--use_domino', int(settings['use_domino']),
            '--n_concurrent_machines', settings['n_concurrent_machines'],
            '--n_shards_per_machine', settings['n_shards_per_machine']
        ]

        # combiner is optional
        combiner = step.get('combiner')
        if combiner is not None:
            cmd_opts.extend([
                '--combine_func', combiner.func_name,
                '--combine_module', combiner.func_globals['__file__']])

        cmd = util.create_cmd(cmd_opts)
        util.wait_cmd(cmd, logger, "Step %d" % i)
    logger.info('All done.')
