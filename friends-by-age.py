from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)
# Parsing (mapping) the input data
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("C:/Users/Charles/SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
# Counting up the sum of friends and number of entries per age
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# Compute averages
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
# Collect and display the results
results = averagesByAge.collect()
for result in results:
    print(result)
