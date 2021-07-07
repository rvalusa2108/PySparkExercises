from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# Creating the Spark Session
sparkSess = SparkSession.builder.master('local[4]').appName('SparkPivotUnpivot').getOrCreate()

# Defining the data schema structure
schema = StructType([StructField('OlympicYear', IntegerType(), False),
                     StructField('Sport', StringType(), False),
                     StructField('Gender', StringType(), False),
                     StructField('Event', StringType(), False),
                     StructField('Medal', StringType(), False),
                     StructField('NOC', StringType(), False),
                     StructField('Athlete', StringType(), False)])

# Create the Spark Dataframe
olympicMedalWinnersDF = sparkSess.read.format('csv').\
    option('header', True).\
    schema(schema).\
    load(r'E:\MyLearning\DataScience\GitHub\PySparkExercises\data\olympic_medal_winners_2016.csv')

olympicMedalWinnersDF.printSchema()

olympicMedalWinnersDF.createOrReplaceTempView('olympicMedalWinnersTbl')

'''
https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-pivot.html
https://blogs.oracle.com/sql/how-to-convert-rows-to-columns-and-back-again-with-sql-aka-pivot-and-unpivot
The PIVOT clause can be specified after the table name or subquery
Syntax:
PIVOT ( { aggregate_expression(3) [ AS aggregate_expression_alias ] } [ , ... ]
     FOR column_list(1) IN ( expression_list(2) ) )

Pivot need three things:
    1. The column that has the values defining the new columns
    2. What these defining values are
    3. What to show in the new columns - eg: count, sum, min etc.,
The value in the new columns must be an aggregate. For example, count, sum, min, etc. 
Place a pivot clause containing these items after the table name, like so:

select * from table
pivot ( 3 for 1 in (2, 2, 2) );
'''

df = sparkSess.sql('''
SELECT * FROM olympicMedalWinnersTbl 
  PIVOT ( count(1) FOR Medal IN ('Gold' GOLD, 'Silver' SILVER, 'Bronze' BRONZE) )
  ORDER BY NOC
  LIMIT 10
  --FETCH FIRST 6 ROWS ONLY
''')

