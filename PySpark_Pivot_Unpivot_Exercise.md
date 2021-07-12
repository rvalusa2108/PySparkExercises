---


---

<h1 id="pyspark-pivot-unpivot-exercise">Pyspark Pivot Unpivot Exercise</h1>
<p>In this exercise we will work on the example of Pivoting and Unpivoting the data using PySpark.<br>
In this example we will be using the olympic_medal_winners_2016.csv dataset.</p>
<blockquote>
<p><strong>Sample Data:</strong><br>
OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE<br>
2016,Archery,M,Men’s Individual,Gold,KOR,KU Bonchan<br>
2016,Archery,M,Men’s Individual,Silver,FRA,VALLADONT Jean-Charles<br>
2016,Archery,M,Men’s Individual,Bronze,USA,ELLISON Brady<br>
2016,Archery,W,Women’s Individual,Gold,KOR,CHANG Hyejin<br>
2016,Archery,W,Women’s Individual,Silver,GER,UNRUH Lisa<br>
2016,Archery,W,Women’s Individual,Bronze,KOR,KI Bobae</p>
</blockquote>
<p><strong>Note:</strong> NOC is the country nominating the Athletes.</p>
<p>From the data above, we will try to find how each country fared overall. To get this we need to convert the table above to the final medal table as below</p>

<table>
<thead>
<tr>
<th>Country</th>
<th>Gold</th>
<th>Silver</th>
<th>Bronze</th>
</tr>
</thead>
<tbody>
<tr>
<td>United States</td>
<td>46</td>
<td>37</td>
<td>38</td>
</tr>
<tr>
<td>Great Britain</td>
<td>27</td>
<td>23</td>
<td>17</td>
</tr>
<tr>
<td>China</td>
<td>26</td>
<td>18</td>
<td>26</td>
</tr>
<tr>
<td>Russia</td>
<td>19</td>
<td>18</td>
<td>19</td>
</tr>
<tr>
<td>Germany</td>
<td>17</td>
<td>10</td>
<td>15</td>
</tr>
</tbody>
</table><p>To do this, we need to count the number of gold, silver and bronze rows for each country. Then create new columns to hold the results.<br>
This can be done by transforming the MEDAL column data into Rows and<br>
populating the count of the medals in category (Gold, Silver, and Bronze), which can be achieved by using the PIVOT clause in PySpark SQL switching rows to columns.</p>
<p><a href="https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-pivot.html">PySpark - Pivot</a></p>
<p>To use this you need three things:</p>
<ol>
<li>The column that has the values defining the new columns</li>
<li>What these defining values are</li>
<li>What to show in the new columns</li>
</ol>
<p>The value in the new columns must be an aggregate. For example, count, sum, min, etc. Place a pivot clause containing these items after the table name, like so:</p>
<blockquote>
<pre><code>select * from table 
   pivot ( 3 for 1 in (2, 2, 2) );
</code></pre>
</blockquote>
<p>So to create the final medal table from the raw data, we need to plug in:</p>
<ol>
<li>We want the medals to become columns. So this is medal.</li>
<li>The values defining the columns are Gold, Silver and Bronze</li>
<li>We need how many rows there are for each colour of medal. i.e. a count(*)</li>
</ol>
<p>So now lets get the data loaded into a pyspark dataframe by importing the required libraries. modules, datatypes, and functions</p>
<blockquote>
<pre><code>from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
# Creating the Spark Session
sparkSess = SparkSession.builder.master('local[4]').appName('SparkPivotUnpivot').getOrCreate()
</code></pre>
</blockquote>
<p>Define the schema for the data set to be loaded into a PySpark dataframe using StructType and StructField classes and create the dataframe.</p>
<blockquote>
<pre><code># Defining the data schema structure
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
</code></pre>
</blockquote>
<p>Print the dataframe schema</p>
<blockquote>
<pre><code>olympicMedalWinnersDF.printSchema()
root
 |-- OlympicYear: integer (nullable = true)
 |-- Sport: string (nullable = true)
 |-- Gender: string (nullable = true)
 |-- Event: string (nullable = true)
 |-- Medal: string (nullable = true)
 |-- NOC: string (nullable = true)
 |-- Athlete: string (nullable = true)
</code></pre>
</blockquote>
<p>Create the TempView on the Dataframe which we can leverage to write the Pivot Sql using PySpark.sql class</p>
<blockquote>
<p>olympicMedalWinnersDF.createOrReplaceTempView(‘olympicMedalWinnersTbl’)</p>
</blockquote>
<p>Based on the syntax mentioned earlier the pivot sql query to list the count of the medals won by each NOC will be as below, in which the 3 new columns will be added based on the items presents in “Medal” column of the dataframe temp view.</p>
<blockquote>
<pre><code>medalPivotDf = sparkSess.sql('''  
SELECT * FROM olympicMedalWinnersTbl
    PIVOT ( count(1) FOR Medal IN ('Gold' GOLD, 'Silver' SILVER, 'Bronze' BRONZE) )
    ORDER BY NOC LIMIT 10 
 ''')
</code></pre>
</blockquote>
<blockquote>
<pre><code>medalPivotDf.show()
Python 3.8.6 | packaged by conda-forge | (default, Dec 26 2020, 04:30:06) [MSC v.1916 64 bit (AMD64)]
Type 'copyright', 'credits' or 'license' for more information
IPython 7.19.0 -- An enhanced Interactive Python. Type '?' for help.
PyDev console: using IPython 7.19.0
21/07/11 20:24:21 WARN CSVHeaderChecker: CSV header does not conform to the schema.
 Header: OLYMPIC_YEAR, SPORT, GENDER, EVENT, MEDAL, NOC, ATHLETE
 Schema: OlympicYear, Sport, Gender, Event, Medal, NOC, Athlete
Expected: OlympicYear but found: OLYMPIC_YEAR
CSV file: file:///E:/MyLearning/DataScience/GitHub/PySparkExercises/data/olympic_medal_winners_2016.csv
+-----------+-------------+------+--------------------+---+--------------------+----+------+------+
|OlympicYear|        Sport|Gender|               Event|NOC|             Athlete|GOLD|SILVER|BRONZE|
+-----------+-------------+------+--------------------+---+--------------------+----+------+------+
|       2016|    Athletics|     M|         Men's 1500m|ALG|   MAKHLOUFI Taoufik|null|     1|  null|
|       2016|    Athletics|     M|          Men's 800m|ALG|   MAKHLOUFI Taoufik|null|     1|  null|
|       2016|      Sailing|     X|      Nacra 17 Mixed|ARG|     Carranza Saroli|   1|  null|  null|
|       2016|         Judo|     W|        Women -48 kg|ARG|        PARETO Paula|   1|  null|  null|
|       2016|       Hockey|     M|                 Men|ARG|           Argentina|   1|  null|  null|
|       2016|      Sailing|     X|      Nacra 17 Mixed|ARG|               Lange|   1|  null|  null|
|       2016|       Tennis|     M|       Men's Singles|ARG|DEL POTRO Juan Ma...|null|     1|  null|
|       2016|    Wrestling|     M|Men's Greco-Roman...|ARM|   ARUTYUNYAN Migran|null|     1|  null|
|       2016|Weightlifting|     M|        Men's +105kg|ARM|        MINASYAN Gor|null|     1|  null|
|       2016|Weightlifting|     M|         Men's 105kg|ARM|   MARTIROSYAN Simon|null|     1|  null|
+-----------+-------------+------+--------------------+---+--------------------+----+------+------+
</code></pre>
</blockquote>
<p>The above result gives the results per athlete, that’s because the implicit group by is applied for all columns not in the pivot clause (i,e., group by applied on columns - OlympicYear, Sport, Gender, Event, NOC, Athlete).</p>
<p>To avoid this, we need to write the inline view that selects just the columns we want in the results i.e., country(NOC) and medal.</p>
<blockquote>
<p>countryMedalsPivotDf = sparkSess.sql(’’’   SELECT * FROM ( SELECT NOC,<br>
Medal FROM olympicMedalWinnersTbl ) PIVOT ( count(1) FOR Medal IN<br>
(‘Gold’ GOLD, ‘Silver’ SILVER, ‘Bronze’ BRONZE) )    ORDER BY NOC<br>
LIMIT 10’’’)</p>
</blockquote>

