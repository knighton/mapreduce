import sys
import imp
import json
from argparse import ArgumentParser, FileType


def parse_args():
    ap = ArgumentParser()
    ap.add_argument('--combine_module', type=str, required=False,
                    help='path to module containing combiner')
    ap.add_argument('--combine_func', type=str, required=False,
                    help='combiner function name')
    ap.add_argument('--input', type=FileType('r'), default=sys.stdin,
                    help='string that input files are prefixed with')
    ap.add_argument('--output', type=FileType('w'), default=sys.stdout,
                    help='string to prefix output files')
    args = ap.parse_args()
    return args


def main():
    args = parse_args()

    # find the combine function.
    combine_module = imp.load_source('combine_module', args.combine_module)
    combine_func = getattr(combine_module, args.combine_func)

    in_fh = args.input
    out_fh = args.output

    last_key = None
    values = []
    for line in in_fh:
        key, value = json.loads(line)
        if key == last_key:
            # extend previous run
            values.append(value)
        else:
            # end previous run
            if values:
                for kv in combine_func(last_key, values):
                    out_fh.write(json.dumps(kv) + '\n')

            # start new run
            last_key = key
            values = [value]
    # dump any remaining values
    if values:
        for kv in combine_func(last_key, values):
            out_fh.write(json.dumps(kv) + '\n')


if __name__ == '__main__':
    main()
