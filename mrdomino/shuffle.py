import heapq
from glob import glob
from argparse import ArgumentParser
from os.path import join as path_join
from itertools import imap
from mrdomino.util import read_files


def parse_args():
    ap = ArgumentParser()
    ap.add_argument('--work_dir', type=str, required=True,
                    help='directory containing files to shuffle')
    ap.add_argument('--n_reduce_shards', type=int, required=True,
                    help='number of reducers to create input files for')
    ap.add_argument('--input_prefix', type=str, default='map.out',
                    help='string that input files are prefixed with')
    ap.add_argument('--output_prefix', type=str, default='reduce.in',
                    help='string to prefix output files')
    args = ap.parse_args()
    return args


def main():

    args = parse_args()

    # count exactly how many input lines we have so we can balance work.
    count_ff = glob(path_join(args.work_dir, args.input_prefix + '_count.*'))
    num_entries = sum(imap(int, read_files(count_ff)))

    in_ff = sorted(glob(path_join(args.work_dir, args.input_prefix + '.*')))
    sources = [open(f, 'r') for f in in_ff]

    n_output_files = args.n_reduce_shards
    out_format = path_join(args.work_dir, args.output_prefix + '.%d')
    outputs = [open(out_format % i, 'w') for i in range(n_output_files)]

    for count, line in enumerate(heapq.merge(*sources)):
        index = count * n_output_files / num_entries
        outputs[index].write(line)

    for source in sources:
        source.close()

    for output in outputs:
        output.close()


if __name__ == "__main__":
    main()
