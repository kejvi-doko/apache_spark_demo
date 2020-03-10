from pyspark import SparkConf, SparkContext
import collections


def load_movies_names():
    movie_names = {}
    with open("C:/SparkCourse/ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


def parse_lines(lines):
    split_array = lines.split()
    return int(split_array[1]), 1


conf = SparkConf().setMaster("local").setAppName("MostFrequentMovieNicer")
sc = SparkContext(conf=conf)

# Distribute the collection from file to all computing nodes
# Be aware for the size of the lookup table
name_dict = sc.broadcast(load_movies_names())

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

mapped_lines = lines.map(parse_lines)

aggregated_data = mapped_lines.reduceByKey(lambda x, y: x + y)

flipped = aggregated_data.map(lambda x: (x[1], x[0]))

sorted_movie = flipped.sortByKey()

sorted_movie_with_name = sorted_movie.map(lambda x: (name_dict.value[x[1]], x[0]))

collected_data = sorted_movie_with_name.collect()

for v in collected_data:
    print('Movie: ' + str(v[0]) + ' Count:' + str(v[1]))
