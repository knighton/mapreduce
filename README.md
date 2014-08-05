mapreduce
=========

Implementation of mapreduce to run on http://dominoup.com.

Example usage (complete example at example.py):

    steps = [
        {
            'mapper': map1,
            'n_mappers': 4,
            'reducer': reduce1,
            'n_reducers': 5,
        },
        {
            'mapper': map2,
            'n_mappers': 10,
            'reducer': reduce2,
            'n_reducers': 7,
        }
    ]

    settings = {
        'use_domino': False,
        'n_concurrent_jobs': 3,
        'input_files': glob.glob('data/short.*'),
        'output_dir': 'out',
    }

    mrdomino.mapreduce(steps, settings)


Map and reduce interface:

    def map(value, increment_counter):
        ...
        yield key, value
    
and

    def reduce(key, values, increment_counter):
        ...
        yield value
