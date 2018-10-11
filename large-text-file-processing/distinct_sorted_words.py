# Given a very large text file that may not fit in available memory,
# create a file that contains the distinct words from the original file
# sorted in the ascending order.
#
# Author: Mihai Nicolae
#
# The output will reside in the file <input-file>_sorted/part-00000

from string import punctuation
from sys import argv
from pyspark import SparkContext, SparkConf
import argparse
import subprocess

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", help="very large input text file")
    args = parser.parse_args()

    conf = SparkConf().setAppName("informatica app")
    sc = SparkContext(conf=conf)

    for item in sorted(sc._conf.getAll()): print(item)

    # read input file into Spark RDD object.
    lines = sc.textFile(args.input_file)
    # split up lines into words based on whitespace.
    words = lines.flatMap(lambda x: x.split())
    # strip trailing punctuation from words.
    words = words.map(lambda x: x.strip(punctuation))
    # remove duplicate words.
    words = words.distinct()
    # create a key, value pair for every distinct word in order
    # to perform sort.
    words = words.map(lambda x: (x,1))
    words = words.sortByKey(ascending=True,numPartitions=None)
    # get keys from the key, value pairs.
    words = words.keys()
    # use coalesce to concatenate the materialized output text files
    # shuffle=True will do the map in parallel but still pass all the data
    # through one reduce node for writing it out.
    words = words.coalesce(1, shuffle=True)
    words.saveAsTextFile(args.input_file + "_sorted")

