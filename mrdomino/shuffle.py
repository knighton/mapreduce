from argparse import ArgumentParser
import glob
import os
import heapq

ap = ArgumentParser()
ap.add_argument('--work_dir', type=str,
                help='directory containing files to shuffle')
ap.add_argument('--n_reduce_shards', type=int,
                help='number of reducers to create input files for')
args = ap.parse_args()

assert args.work_dir
assert args.n_reduce_shards

# count exactly how many input lines we have so we can balance work.
num_entries = 0
count_ff = glob.glob(os.path.join(args.work_dir, 'map.out_count.*'))
for f in count_ff:
    with open(f, 'r') as fh:
        num_entries += int(fh.read())

in_ff = sorted(glob.glob(os.path.join(args.work_dir, 'map.out.*')))
sources = [open(f, 'r') for f in in_ff]

out_format = os.path.join(args.work_dir, 'reduce.in.%d')
out_ff = map(lambda i: open(out_format % i, 'w'),
             range(args.n_reduce_shards))

count = 0
for line in heapq.merge(*sources):
    index = count * len(out_ff) / num_entries
    out_ff[index].write(line)
    count += 1
