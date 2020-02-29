from pyspark import SparkConf, SparkContext
import collections
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

# Option 1

# lines = sc.textFile("file:///SparkCourse/book.txt")
#
# word_list = lines.flatMap(lambda x: x.split())
#
# word_list = word_list.map(lambda x: (x, 1))
#
# count_per_word = word_list.reduceByKey(lambda x, y: x + y)
#
# results = count_per_word.collect()
#
# for result in results:
#     print(result[0] + ' occurred ' + str(result[1]))

print('--------------------------------------------')

# Option 2

# lines = sc.textFile("file:///SparkCourse/book.txt")
#
# word_list = lines.flatMap(lambda x: x.split())
#
# word_count = word_list.countByValue()
#
# for word, count in word_count.items():
#     clean_word = word.encode('ascii', 'ignore')
#     if clean_word:
#         print(clean_word, count)

print('--------------------------------------------')


# Option 3

# def normalize(text):
#     re.compile(r'\W+', re.UNICODE).split(text.lower())
#
#
#
# lines = sc.textFile("file:///SparkCourse/book.txt")
#
# word_list = lines.flatMap(normalize)
#
# word_count = word_list.countByValue()
#
# for word, count in word_count.items():
#     clean_word = word.encode('ascii', 'ignore')
#     if clean_word:
#         print(clean_word, count)

print('--------------------------------------------')


# Option 4

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


input = sc.textFile("file:///SparkCourse/book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(word.decode() + ":\t\t" + count)
