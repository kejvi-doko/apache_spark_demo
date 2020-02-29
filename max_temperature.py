from pyspark import SparkConf, SparkContext
import collections


def parseLine(line):
    fields = line.split(',')
    stationId = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return stationId, entryType, temperature


conf = SparkConf().setMaster("local").setAppName("FakeFriends")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
filteredLines = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = parsedLines.map(lambda x: (x[0], x[2]))
min_temps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = min_temps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
