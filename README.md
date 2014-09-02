mrdomino
========

Implementation of mapreduce to run on http://dominoup.com.

Example usage (see complete example at examples/example.py):

    from mrdomino import mapreduce, MRStep, MRSettings

    steps = [
        MRStep(
            mapper=map1,
            combiner=combine1,
            reducer=reduce1,
            n_mappers=8,
            n_reducers=6
        ),
        MRStep(
            mapper=map2,
            combiner=combine2,
            reducer=reduce2,
            n_mappers=4,
            n_reducers=3
        )
    ]
    settings = MRSettings(
        input_files=glob.glob('data/*.gz'),
        output_dir='out',
        tmp_dir='tmp',
        use_domino=False,
        n_concurrent_machines=3,
        n_shards_per_machine=3
    )
    mrdomino.mapreduce(steps, settings)


Map and reduce interface:

    def map(_, value, increment_counter):
        ...
        yield key, value

and

    def reduce(key, values, increment_counter):
        ...
        yield key, value
