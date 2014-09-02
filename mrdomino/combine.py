import sys
import imp
import json
from os.path import join as path_join
from argparse import ArgumentParser, FileType
from mrdomino import logger
from mrdomino.util import MRCounter


def parse_args():
    ap = ArgumentParser()
    ap.add_argument('--combine_module', type=str, required=False,
                    help='path to module containing combiner')
    ap.add_argument('--combine_func', type=str, required=False,
                    help='combiner function name')
    ap.add_argument('--input', type=FileType('r'), default=sys.stdin,
                    help='string that input files are prefixed with')
    ap.add_argument('--work_dir', type=str, required=True,
                    help='directory containing map output files')
    ap.add_argument('--output_prefix', type=str, default='map.out',
                    help='string to prefix output files')
    ap.add_argument('--shard', type=int, required=True,
                    help='which shart are we at')

    args = ap.parse_args()
    return args


def main():
    args = parse_args()

    # find the combine function.
    combine_module = imp.load_source('combine_module', args.combine_module)
    combine_func = getattr(combine_module, args.combine_func)

    # the counters.
    counters = MRCounter()

    in_fh = args.input
    out_fn = path_join(args.work_dir, args.output_prefix + '.%d' % args.shard)
    logger.info("combiner output -> {}".format(out_fn))

    last_key = None
    values = []

    count_written = 0
    count_seen = 0
    with open(out_fn, 'w') as out_fh:
        for line in in_fh:
            count_seen += 1
            key, value = json.loads(line)
            if key == last_key:
                # extend previous run
                values.append(value)
            else:
                # end previous run
                if values:
                    for kv in combine_func(last_key, values, counters.incr):
                        count_written += 1
                        out_fh.write(json.dumps(kv) + '\n')

                # start new run
                last_key = key
                values = [value]
        # dump any remaining values
        if values:
            for kv in combine_func(last_key, values, counters.incr):
                count_written += 1
                out_fh.write(json.dumps(kv) + '\n')

    counters.incr("combiner", "seen", count_seen)
    counters.incr("combiner", "written", count_written)

    # write out the counters to file.
    f = path_join(args.work_dir, 'combine.counters.%d' % args.shard)
    logger.info("combiner counters -> {}".format(f))
    with open(f, 'w') as fh:
        fh.write(counters.serialize())

    # write how many entries were written for reducer balancing purposes.
    f = path_join(args.work_dir, args.output_prefix + '_count.%d' % args.shard)
    logger.info("combiner lines written -> {}".format(f))
    with open(f, 'w') as fh:
        fh.write(str(count_written))


if __name__ == '__main__':
    main()
