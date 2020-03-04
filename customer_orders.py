from pyspark import SparkConf, SparkContext
import collections


def parseLine(line):
    fields = line.split(',')
    order_id = int(fields[0])
    order_total = float(fields[2])
    return order_id, order_total


conf = SparkConf().setMaster("local").setAppName("OrderTotal")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")

orders_rdd = lines.map(parseLine)

aggregated_orders = orders_rdd.reduceByKey(lambda x, y: x + y)

sorted_orders_by_total = aggregated_orders.map(lambda x: (x[1], x[0])).sortByKey()

results = sorted_orders_by_total.collect()


for result in results:
    print(str(result[1]) + ' - ' + str(result[0]))
