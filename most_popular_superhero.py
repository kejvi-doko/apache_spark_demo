from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("MostPopularSuperHero")
sc = SparkContext(conf=conf)


def countCoOccurences(line):
    elements = line.split()
    return int(elements[0]), len(elements) - 1


def parseNames(line):
    fields = line.split("\"")
    return int(fields[0]), fields[1].encode('utf8')


names = sc.textFile("file:///SparkCourse/Marvel-names.txt")
names_rdd = names.map(parseNames)

lines = sc.textFile("file:///SparkCourse/Marvel-graph.txt")
pairings = lines.map(countCoOccurences)

totalFiendsByCharacter = pairings.reduceByKey(lambda x, y: x + y)
flipped = totalFiendsByCharacter.map(lambda x: (x[1], x[0]))

mostPopular = flipped.max()

mostPopularName = names_rdd.lookup(mostPopular[1])[0]

print(mostPopularName.decode('utf-8') + " is the most popular superhero, with " +
      str(mostPopular[0]) + " co-appearances.")
