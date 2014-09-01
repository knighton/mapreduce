from argparse import ArgumentParser
import imp
import os
import time
from itertools import imap
from mrdomino import EXEC_SCRIPT, logger
from mrdomino.util import MRCounter, create_cmd, read_files, wait_cmd


def parse_args():
    ap = ArgumentParser()
    ap.add_argument('--input_files', type=str, nargs='+',
                    help='list of input files to mappers')
    ap.add_argument('--output_dir', type=str, default='out',
                    help='directory to write output files to')
    ap.add_argument('--work_dir', type=str, required=True,
                    help='temporary working directory')

    ap.add_argument('--map_module', type=str, required=True)
    ap.add_argument('--map_func', type=str, required=True)
    ap.add_argument('--reduce_module', type=str, required=True)
    ap.add_argument('--reduce_func', type=str, required=True)
    ap.add_argument('--combine_module', type=str, required=False, default=None)
    ap.add_argument('--combine_func', type=str, required=False, default=None)

    ap.add_argument('--n_map_shards', type=int,
                    help='number of map shards')
    ap.add_argument('--n_reduce_shards', type=int, default=10,
                    help='number of reduce shards')

    ap.add_argument('--step_idx', type=int, required=True,
                    help='Index of this step (zero-base)')
    ap.add_argument('--total_steps', type=int, required=True,
                    help='total number of steps')

    ap.add_argument('--use_domino', type=int, default=1,
                    help='which platform to run on (local or domino)')
    ap.add_argument('--n_concurrent_machines', type=int, default=2,
                    help='maximum number of domino jobs to be running at once')
    ap.add_argument('--n_shards_per_machine', type=int, default=1,
                    help='number of processes to spawn per domino job (-1 for all)')

    ap.add_argument('--poll_done_interval_sec', type=int, default=45,
                    help='interval between successive checks that we are done')

    args = ap.parse_args()

    # verify that we have input for each mapper.
    if args.n_map_shards is None:
        args.n_map_shards = len(args.input_files)

    # verify functions exist.
    module = imp.load_source('module', args.map_module)
    getattr(module, args.map_func)
    module = imp.load_source('module', args.reduce_module)
    getattr(module, args.reduce_func)

    return args


class ShardState(object):
    NOT_STARTED = 0
    IN_PROGRESS = 1
    DONE = 2


def combine_counters(work_dir, n_map_shards, n_reduce_shards):
    ff = map(lambda (work_dir, shard):
             os.path.join(work_dir, 'map.counters.%d' % shard),
             zip([work_dir] * n_map_shards, range(n_map_shards)))
    ff += map(lambda (work_dir, shard):
              os.path.join(work_dir, 'reduce.counters.%d' % shard),
              zip([work_dir] * n_reduce_shards, range(n_reduce_shards)))
    return MRCounter.sum(
        imap(MRCounter.deserialize, read_files(filter(os.path.exists, ff))))


def update_shards_done(args, done_pattern, num_shards, use_domino,
                       shard2state):
    """go to disk and find out which shards are completed."""
    if args.use_domino:
        os.system('domino download')
    for i in range(num_shards):
        f = done_pattern % i
        if os.path.isfile(f):
            shard2state[i] = ShardState.DONE


def are_all_shards_done(shard2state):
    return list(set(shard2state.itervalues())) == [ShardState.DONE]


def get_shard_groups_to_start(
        n_concurrent_machines, n_shards_per_machine, shard2state):
    """get the list of shards to start now.  update state accordingly."""
    # get the state of each domino job (group of shards).
    shards = sorted(shard2state)
    machines = []
    for i in range(0, len(shards), n_shards_per_machine):
        machine_shards = shards[i:i + n_shards_per_machine]
        machine_status = min(map(lambda shard: shard2state[shard],
                                 machine_shards))
        machines.append(machine_status)

    # get how many domino jobs to start up.
    n_machines_in_progress = \
        len(filter(lambda m: m == ShardState.IN_PROGRESS, machines))
    n_todos = n_concurrent_machines - n_machines_in_progress

    # get up to n_todos domino jobs to start.
    start_me = []
    count = 0
    for i, m in enumerate(machines):
        if m == ShardState.NOT_STARTED:
            machine_shards = range(i * n_shards_per_machine,
                                   (i + 1) * n_shards_per_machine)
            machine_shards = filter(lambda n: n < len(shards), machine_shards)
            start_me.append(machine_shards)
            count += 1
            if count == n_todos:
                break

    return start_me


def show_shard_state(shard2state, n_shards_per_machine):
    shards = sorted(shard2state)
    output = ['Shard state:']
    for i in range(0, len(shards), n_shards_per_machine):
        machine_shards = shards[i:i + n_shards_per_machine]
        output.append('%s' % map(lambda i: shard2state[i], machine_shards))
    return ' '.join(output)


def schedule_machines(args, command, done_file_pattern, n_shards):

    def wrap_cmd(command, use_domino):
        if use_domino:
            pre = 'domino run %s ' % EXEC_SCRIPT
            post = ''
        else:
            pre = '%s ' % EXEC_SCRIPT
            post = ' &'
        return '%s%s%s' % (pre, command, post)

    shard2state = dict(zip(
        range(n_shards),
        [ShardState.NOT_STARTED] * n_shards))

    while True:
        # go to disk and look for shard done files.
        update_shards_done(args, done_file_pattern, n_shards, args.use_domino,
                           shard2state)

        logger.info(show_shard_state(shard2state, args.n_shards_per_machine))

        if are_all_shards_done(shard2state):
            break

        # if we can start any more domino jobs (per n_concurrent_machines
        # restriction), get the ones to start.
        start_me = get_shard_groups_to_start(
            args.n_concurrent_machines, args.n_shards_per_machine, shard2state)

        # start the jobs.
        if start_me:
            logger.info('Starting shard groups: %s' % start_me)
        for shards in start_me:
            # execute command.
            s = command % ','.join(map(str, shards))
            s = wrap_cmd(s, args.use_domino)
            logger.info("Starting process: {}".format(s))
            os.system(s)

            # note them as started.
            for shard in shards:
                shard2state[shard] = ShardState.IN_PROGRESS

        # wait to poll.
        time.sleep(args.poll_done_interval_sec)


def main():

    args = parse_args()
    logger.info('Mapreduce step: %s' % args)

    logger.info('%d input files.' % len(args.input_files))

    work_dir = args.work_dir
    logger.info('Working directory: %s' % work_dir)

    logger.info('Starting %d mappers.' % args.n_map_shards)

    cmd_opts = {
        'step_idx': args.step_idx,
        'total_steps': args.total_steps,
        'shards': '%s',
        'n_shards': args.n_map_shards,
        'input_files': ' '.join(args.input_files),
        'map_module': args.map_module,
        'map_func': args.map_func,
        'work_dir': work_dir
    }
    if args.combine_module is not None:
        cmd_opts['combine_module'] = args.combine_module
    if args.combine_func is not None:
        cmd_opts['combine_func'] = args.combine_func

    cmd = create_cmd('mrdomino.map_one_machine', cmd_opts)
    schedule_machines(
        args,
        command=cmd,
        done_file_pattern=os.path.join(work_dir, 'map.done.%d'),
        n_shards=args.n_map_shards)

    counter = combine_counters(
        work_dir, args.n_map_shards, args.n_reduce_shards)
    logger.info(counter.show())

    # shuffle mapper outputs to reducer inputs.
    logger.info('Shuffling data.')
    cmd = create_cmd(EXEC_SCRIPT + ' mrdomino.shuffle', {
        'work_dir': work_dir,
        'input_prefix': 'map.out',
        'output_prefix': 'reduce.in',
        'n_reduce_shards': args.n_reduce_shards
    })
    wait_cmd(cmd, logger, "Shuffling")

    logger.info('Starting %d reducers.' % args.n_reduce_shards)
    cmd = create_cmd('mrdomino.reduce_one_machine', {
        'step_idx': args.step_idx,
        'total_steps': args.total_steps,
        'shards': '%s',
        'n_shards': args.n_reduce_shards,
        'reduce_module': args.reduce_module,
        'with_combiner': args.combine_func,
        'input_prefix': 'reduce.in',
        'reduce_func': args.reduce_func,
        'work_dir': work_dir,
    })
    schedule_machines(
        args,
        command=cmd,
        done_file_pattern=os.path.join(work_dir, 'reduce.done.%d'),
        n_shards=args.n_reduce_shards)

    counter = combine_counters(
        work_dir, args.n_map_shards, args.n_reduce_shards)
    logger.info(counter.show())

    if args.step_idx == args.total_steps - 1:

        logger.info('Final reduce')
        cmd = create_cmd('mrdomino.reduce_one_machine', {
            'step_idx': args.step_idx,
            'total_steps': args.total_steps,
            'shards': '%s',
            'n_shards': 1,
            'reduce_module': args.reduce_module,
            'reduce_func': args.reduce_func,
            'glob_prefix': 'reduce.out',
            'work_dir': work_dir,
            'output_dir': args.output_dir
        })
        schedule_machines(
            args,
            command=cmd,
            done_file_pattern=os.path.join(args.output_dir, 'reduce.done.%d'),
            n_shards=1)

    # done.
    logger.info('Mapreduce step done.')


if __name__ == '__main__':
    main()
