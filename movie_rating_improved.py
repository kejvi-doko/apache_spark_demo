import sys
from pyspark import SparkConf, SparkContext
from math import sqrt


def loadMovieNames():
    movieNames = {}

    with open("C:/SparkCourse/ml-100k/u.item", encoding='ascii', errors='ignore') as f:
        for line in f:
            movie_genres = []
            fields = line.split('|')
            for index in range(5, len(fields)):
                movie_genres.append(int(fields[index]))
            movieNames[int(fields[0])] = fields[1], movie_genres
    return movieNames


# Python 3 doesn't let you pass around unpacked tuples,
# so we explicitly extract the ratings now.
def makePairs(userRatings):
    ratings = userRatings[1]
    (movie1, rating1, genre1) = ratings[0]
    (movie2, rating2, genre2) = ratings[1]
    return (movie1, movie2), (rating1, rating2, genre1, genre2)


def filterDuplicates(userRatings):
    ratings = userRatings[1]
    (movie1, rating1, genre1) = ratings[0]
    (movie2, rating2, genre2) = ratings[1]
    return movie1 < movie2


def filterBadRating(ratting):
    currentRatting = ratting[1]
    return currentRatting[1] > 2


def map_genre(line):
    fields = line.split('|')
    movie_genres = {}

    temp = []
    for index in range(5, len(fields)):
        temp.append(fields[index])

    movie_genres[int(fields[0])] = temp

    return movie_genres


def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY, genreX, genreY in ratingPairs:
        sum_xx += ratingX * ratingX * computeCosineSimilarityGenre(genreX,genreX)
        sum_yy += ratingY * ratingY * computeCosineSimilarityGenre(genreY,genreY)
        sum_xy += ratingX * ratingY * computeCosineSimilarityGenre(genreX,genreY)
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if denominator:
        score = (numerator / (float(denominator)))

    return score, numPairs


def computeCosineSimilarityGenre(generX, gernreY):
    sum_xx = sum_yy = sum_xy = 0
    for index in range(0,len(generX)):
        sum_xx += generX[index] * generX[index]
        sum_yy += gernreY[index] * gernreY[index]
        sum_xy += generX[index] * gernreY[index]

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if denominator:
        score = (numerator / (float(denominator)))

    return score

conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilaritiesImproved")
sc = SparkContext(conf=conf)

print("\nLoading movie names...")
nameDict = sc.broadcast(loadMovieNames())

data = sc.textFile("file:///SparkCourse/ml-100k/u.data")

# Map ratings to key / value pairs: user ID => movie ID, rating, genre array
ratings = data.map(lambda l: l.split()).map(
    lambda l: (int(l[0]), (int(l[1]), float(l[2]), nameDict.value[int(l[1])][1])))

filtered_ratings = ratings.filter(filterBadRating)

# movie_genre_result = movie_genre.collect()
# for mvgen in movie_genre_result:
#     print (mvgen)

# Emit every movie rated together by the same user.
# Self-join to find every combination.
joinedRatings = filtered_ratings.join(filtered_ratings)

# At this point our RDD consists of userID => ((movieID, rating, genre), (movieID, rating, genre))
#
# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# Now key by (movie1, movie2) pairs.
moviePairs = uniqueJoinedRatings.map(makePairs)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = moviePairs.groupByKey()

# We now have (movie1, movie2) = > (rating1, rating2, genre1, genre2), (rating1, rating2, genre1, genre2) ...
# Can now compute similarities.
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

# Save the results if desired
# moviePairSimilarities.sortByKey()
# moviePairSimilarities.saveAsTextFile("movie-sims")

# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 70

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
                                                       (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
                                                       and pairSim[1][0] > scoreThreshold and pairSim[1][
                                                           1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending=False).take(10)

    print("Top 10 similar movies for " + nameDict.value[movieID][0])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict.value[similarMovieID][0] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
