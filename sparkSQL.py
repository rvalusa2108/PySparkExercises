from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col, array_contains
from pyspark.sql.functions import count, sum, avg, max

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

schema = StructType() \
    .add("ID", IntegerType(), True) \
    .add("Name", StringType(), True) \
    .add("Age", IntegerType(), True) \
    .add("NoOfFriends", IntegerType(), True)

fakeFriendsDF = spark.read.format("csv") \
    .option("header", False) \
    .schema(schema) \
    .load(r'E:\MyLearning\DataScience\GitHub\PySparkExercises\data\fakefriends.csv')

fakeFriendsDF.createOrReplaceTempView("fakeFriendsTbl")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM fakeFriendsTbl WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
    print(teen)

# We can also use functions instead of SQL queries:
fakeFriendsDF.groupBy("age").count().orderBy("age").show()

friendsAgeCount = spark.sql('SELECT age, count(1) FROM fakeFriendsTbl GROUP BY age ORDER BY age')
friendsAgeCount.show(False)

# Multiple aggregate functions using agg function of PySpark
fakeFriendsDF.groupBy("age").agg(count('age').alias('ageCount'),
                                 sum('age').alias('ageSum'),
                                 avg('age').alias('ageAvg'),
                                 max('age').alias('ageMax')).orderBy("age").show()

spark.stop()

