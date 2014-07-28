mapreduce
=========

Implementation of mapreduce to run on http://dominoup.com.

To use, provide a python file to mapreduce.py that implements

    def map(value):
        ...
        yield key, value
    
and

    def reduce(key, values):
        ...
        yield value
