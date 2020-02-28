from pyspark import SparkConf, SparkContext
import collections


def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return age, num_friends


conf = SparkConf().setMaster("local").setAppName("FakeFriends")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
# Transform line data to key value tuples
rdd = lines.map(parseLine)
# Apply function where key is not changed, only the value is passed on function
rdd = rdd.mapValues(lambda x: (x, 1))
# Run the function for every element grouped by key
totalsByAge = rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

result = averagesByAge.collect()

for fr in result:
    print('Age: ' + str(fr[0]) + 'Avg Friends ' + str(fr[1]))

