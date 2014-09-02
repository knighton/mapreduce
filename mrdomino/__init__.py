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


class MRStep(object):
    def __init__(self, mapper, reducer, combiner=None, n_mappers=2,
                 n_reducers=2):

        # do some basic type checking to verify that we pass callables.
        assert hasattr(mapper, '__call__')
        self.mapper = mapper
        assert hasattr(reducer, '__call__')
        self.reducer = reducer
        assert combiner is None or hasattr(combiner, '__call__')
        self.combiner = combiner
        assert isinstance(n_mappers, int)
        self.n_mappers = n_mappers
        assert isinstance(n_reducers, int)
        self.n_reducers = n_reducers

    @property
    def mapper_func(self):
        return self.mapper.func_name

    @property
    def mapper_module(self):
        return self.mapper.func_globals['__file__']

    @property
    def combiner_func(self):
        return self.combiner.func_name

    @property
    def combiner_module(self):
        return self.combiner.func_globals['__file__']

    @property
    def reducer_func(self):
        return self.reducer.func_name

    @property
    def reducer_module(self):
        return self.reducer.func_globals['__file__']


class MRSettings(object):
    def __init__(self, input_files, output_dir, tmp_dir, use_domino=False,
                 n_concurrent_machines=2, n_shards_per_machine=4):

        assert isinstance(input_files, list)
        self.input_files = input_files
        assert isinstance(output_dir, str)
        self.output_dir = output_dir
        assert isinstance(tmp_dir, str)
        self.tmp_dir = tmp_dir
        assert isinstance(use_domino, bool)
        self.use_domino = use_domino
        assert isinstance(n_concurrent_machines, int)
        self.n_concurrent_machines = n_concurrent_machines
        assert isinstance(n_shards_per_machine, int)
        self.n_shards_per_machine = n_shards_per_machine


def mapreduce(steps, settings):

    step_count = len(steps)

    # if temporary directory root does not exist, create one
    tmp_root = settings.tmp_dir
    if not os.path.exists(tmp_root):
        os.makedirs(tmp_root)
    tmp_dirs = [mkdtemp(dir=tmp_root, prefix="step%d." % i)
                for i in range(step_count)]

    input_file_lists = [settings.input_files]
    for step, out_dir in zip(steps, tmp_dirs):
        n_reducers = step.n_reducers
        reduce_format = os.path.join(out_dir, 'reduce.out.%d')
        ff = [reduce_format % n for n in range(n_reducers)]
        input_file_lists.append(ff)

    # if output directory root does not exist, create one
    output_dir = settings.output_dir
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
            '--map_module', step.mapper_module,
            '--map_func', step.mapper_func,
            '--n_map_shards', step.n_mappers,
            '--reduce_module', step.reducer_module,
            '--reduce_func', step.reducer_func,
            '--n_reduce_shards', step.n_reducers,
            '--use_domino', int(settings.use_domino),
            '--n_concurrent_machines', settings.n_concurrent_machines,
            '--n_shards_per_machine', settings.n_shards_per_machine
        ]

        # combiner is optional
        if step.combiner is not None:
            cmd_opts.extend([
                '--combine_func', step.combiner_func,
                '--combine_module', step.combiner_module])

        cmd = util.create_cmd(cmd_opts)
        util.wait_cmd(cmd, logger, "Step %d" % i)
    logger.info('All done.')
