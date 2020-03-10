from pyspark import SparkConf, SparkContext
import collections


def parse_lines(lines):
    split_array = lines.split()
    return int(split_array[1]), 1


conf = SparkConf().setMaster("local").setAppName("MostFrequentMovie")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

mapped_lines = lines.map(parse_lines)

aggregated_data = mapped_lines.reduceByKey(lambda x, y: x + y)

flipped = aggregated_data.map(lambda x: (x[1], x[0]))

sorted_movie = flipped.sortByKey()

collected_data = sorted_movie.collect()

for v in collected_data:
    print('Movie: ' + str(v[1]) + ' Count:' + str(v[0]))
