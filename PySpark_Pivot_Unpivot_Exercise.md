---


---

<h1 id="pyspark-pivot-unpivot-exercise">Pyspark Pivot Unpivot Exercise</h1>
<p>In this exercise we will work on the example of Pivoting and Unpivoting the data using PySpark.<br>
In this example we will be using the olympic_medal_winners_2016.csv dataset.</p>
<p><strong>Sample Data:</strong><br>
OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE<br>
2016,Archery,M,Men’s Individual,Gold,KOR,KU Bonchan<br>
2016,Archery,M,Men’s Individual,Silver,FRA,VALLADONT Jean-Charles<br>
2016,Archery,M,Men’s Individual,Bronze,USA,ELLISON Brady<br>
2016,Archery,W,Women’s Individual,Gold,KOR,CHANG Hyejin<br>
2016,Archery,W,Women’s Individual,Silver,GER,UNRUH Lisa<br>
2016,Archery,W,Women’s Individual,Bronze,KOR,KI Bobae</p>
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
<pre><code>select * from table 
   pivot ( 3 for 1 in (2, 2, 2) );
</code></pre>
<p>So to create the final medal table from the raw data, we need to plug in:</p>
<ol>
<li>We want the medals to become columns. So this is medal.</li>
<li>The values defining the columns are Gold, Silver and Bronze</li>
<li>We need how many rows there are for each colour of medal. i.e. a count(*)</li>
</ol>

