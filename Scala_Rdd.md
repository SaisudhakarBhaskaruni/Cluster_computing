<p><a target="_blank" href="https://app.eraser.io/workspace/0dKIJwDuDzX7EJDEmgWJ" id="edit-in-eraser-github-link"><img alt="Edit in Eraser" src="https://firebasestorage.googleapis.com/v0/b/second-petal-295822.appspot.com/o/images%2Fgithub%2FOpen%20in%20Eraser.svg?alt=media&amp;token=968381c8-a7e7-472a-8ed6-4a6626da5501"></a></p>

create an RDD and apply a map transformation to square a list of numbers

```
scala> val num = List(1,2,3,4,5)
num: List[Int] = List(1, 2, 3, 4, 5)

scala> val rdd = sc.parallelize(num)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[5] at parallelize at <console>:24

scala> val rdd1 = rdd.map(x=> x * x)
rdd1: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[6] at map at <console>:23

scala> rdd1.collect()
res2: Array[Int] = Array(1, 4, 9, 16, 25)
```
Print only Even Numbers in the Rdd

```
scala> val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8))
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[7] at parallelize at <console>:23

scala> val even = rdd.filter(x=>x*2)
<console>:23: error: type mismatch;
 found   : Int
 required: Boolean
       val even = rdd.filter(x=>x*2)
                                 ^

scala> val even = rdd.filter(x=> x % 2==0)
even: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[8] at filter at <console>:23

scala> even.collect()
res4: Array[Int] = Array(2, 4, 6, 8)
```
A sum of all elements in Rdd

```
scala> val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8))
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[9] at parallelize at <console>:23

scala> val sum = rdd.reduce(_+_)
sum: Int = 36
```
List("Hello world", "Scala Spark") split the sentences into words

```
scala> val sentence = List("hello world","scala spark")
sentence: List[String] = List(hello world, scala spark)

scala> val words = sentence.flatMap(sentence => sentence.split(" "))
words: List[String] = List(hello, world, scala, spark)

scala> words.foreach(println)
hello
world
scala
spark
```
count the number of elements in rdd

```
> val counting = rdd.count()
counting: Long = 8
```
List(1,2,3,4,4,5,5,6) remove the duplicates from an Rdd

```
scala> val removeNumbers = List(1,2,3,3,4,4,5,6,6)
removeNumbers: List[Int] = List(1, 2, 3, 3, 4, 4, 5, 6, 6)

scala> val dis = removeNumbers.distinct
dis: List[Int] = List(1, 2, 3, 4, 5, 6)

scala>
```
group numbers based on even and odd

```
:paste
// Entering paste mode (ctrl-D to finish)

val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8))
val groupRdd = rdd.groupBy( x => if(x % 2 == 0) "Even" else "Odd")
val groupNum = groupRdd.collect()
groupNum.foreach{case (key, values) =>
  println(s"$key: ${values.mkString(", ")}")}

// Exiting paste mode, now interpreting.

Even: 2, 4, 6, 8
Odd: 1, 3, 5, 7
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[6] at parallelize at <pastie>:25
groupRdd: org.apache.spark.rdd.RDD[(String, Iterable[Int])] = ShuffledRDD[8] at groupBy at <pastie>:26
groupNum: Array[(String, Iterable[Int])] = Array((Even,CompactBuffer(2, 4, 6, 8)), (Odd,CompactBuffer(1, 3, 5, 7)))

scala>
```
List("apple","Banana","apple","orange","banana","apple") count the occurrence of the words in rdd

```
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[13] at paralleliz
scala> val words = List("apple","banana","apple","banana","orange")
words: List[String] = List(apple, banana, apple, banana, orange)

scala> val rdd = sc.parallelize(words)
rdd: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[25] at parallelize at <console>:24

scala> val wordCounts = rdd.map(words => words.toLowerCase).map(words=>(words,1)).reduceByKey(_+_).collect()
wordCounts: Array[(String, Int)] = Array((orange,1), (apple,2), (banana,2))

scala> wordsCounts.foreach(println)
<console>:23: error: not found: value wordsCounts
       wordsCounts.foreach(println)
       ^

scala> wordCounts.foreach(println)
(orange,1)
(apple,2)
(banana,2)
```
create an RDD from a list of numbers and multiply each number by List(1,2,3,4,5)

```
val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8))
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[29] at parallelize at <console>:23

scala> val multi = rdd.reduce(_*_)
multi: Int = 40320

scala>
```
keep numbers greater than 3 in an RDD

```
scala> val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8))
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[30] at parallelize at <console>:23

scala> val greater = rdd.filter(x=>x >3)
greater: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[31] at filter at <console>:23

scala> greater.collect()
res15: Array[Int] = Array(4, 5, 6, 7, 8)
```
find the first element in rdd

```
scala> rdd.first()
res19: Int = 1

scala>
```
combine two rdd .list(1,2,3) list(4,5,6)

```
scala> val rdd1 = sc.parallelize(List(1,2,3))
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[36] at parallelize at <console>:23

scala> val rdd2 = sc.parallelize(List(4,5,6)
     | )
rdd2: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[37] at parallelize at <console>:23

scala> val combined = rdd1.union(rdd2)
combined: org.apache.spark.rdd.RDD[Int] = UnionRDD[38] at union at <console>:24

scala> combined.collect().foreach(println)
1
2
3
4
5
6
```
find common elements in two rdd list(1,2,3,4) list(3,4,5,6)

```
rdd1.intersection(rdd2).collect()
res20: Array[Int] = Array(3, 4)
```
sort the elements of an rdd in ascending order list (9,5,6,3,77)

```
val asc = List(9,5,6,3,77)
asc: List[Int] = List(9, 5, 6, 3, 77)

scala> val sorted = asc.sortBy(x=>x)
sorted: List[Int] = List(3, 5, 6, 9, 77)

scala> println(sorted)
List(3, 5, 6, 9, 77)
```
reduce the numbers of partitions in an rdd ("a", "b" ,"c") list(1,2,3)

```
val rdd =sc.parallelize(List("a","b","c"))
rdd: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[7] at parallelize at <console>:23

scala> val rdd2 = sc.parallelize(List(4,5,6)
     | )
rdd2: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[8] at parallelize at <console>:23

scala> val reducePartition = rdd.union(rdd2.map(_.toString))
reducePartition: org.apache.spark.rdd.RDD[String] = UnionRDD[10] at union at <console>:24

scala> val reducepar = reducePartition.coalesce(1)
reducepar: org.apache.spark.rdd.RDD[String] = CoalescedRDD[11] at coalesce at <console>:23

scala> val result = reducepar.collect()
result: Array[String] = Array(a, b, c, 4, 5, 6)
```
find the average of numbers in an rdd list(10,20,30,40,50)

```
val rdd = sc.parallelize(List(10,20,30,40,50))
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[2] at parallelize at <console>:23

scala> val sum = rdd.reduce(_+_)
sum: Int = 150

scala> val count = rdd.count()
count: Long = 5

scala> val average = sum.toDouble/count
average: Double = 30.0

scala> println(average)
30.0

scala>
```
# Scala Functions and formulas
```
Scala formulas

1.val a = Array(1,2,3,4,5,6,7,8,9,10)
a: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
2. val b = a.map(a=>a*2)
b: Array[Int] = Array(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
3.val list1 = List("a","b","c")
list1: List[String] = List(a, b, c)
4.val l1 = list1:+"d"
l1: List[String] = List(a, b, c, d)
5.val l2 = "e"+l1
l2: String = eList(a, b, c, d)
6. val l4 = Vector(1,2,3)
l4: scala.collection.immutable.Vector[Int] = Vector(1, 2, 3)
7. val l5 = l4:+5
l5: scala.collection.immutable.Vector[Int] = Vector(1, 2, 3, 5)
8. val l6 = 6+:l5
l6: scala.collection.immutable.Vector[Int] = Vector(6, 1, 2, 3, 5)
9. val l7 = l5 ++ l6
l7: scala.collection.immutable.Vector[Int] = Vector(1, 2, 3, 5, 6, 1, 2, 3, 5)
10.val l8 = Stream("abc","def","ghi")
l8: scala.collection.immutable.Stream[String] = Stream(abc, ?)
11.list1.head
res4: String = a
12. list1.tail
res5: List[String] = List(b, c)
13.l8.head
res6: String = abc
14. l8.tail
res7: scala.collection.immutable.Stream[String] = Stream(def, ?)
15.val list = (1 to 1000000000).toList //OutofMemoryException
16. val list1 = (1 to 1000000000).toStream
17.sc.textfile (path of textfile), sc.parallelize()   sc-sparkcontext - Basic entry point, spark-sparksession - unified entry point 
18.Collections - Map, Set, Sequence - Array, ArrayBuffer, Vector, List, LazyList
19.val a = Array(1,2,3,4,5,6,7,8,9,10)
a: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
20. val rdd1 = sc.parallelize(a) //RDD - Resilient Distributed Dataset
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:24
21. Transformations(Narrow(update,alter,delete),wide(groupbyKey,join)) - map,filter,flatmap,groupby,sortby
a) val b = a.map(a=>a*2)
   b: Array[Int] = Array(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
b) val x = sc.parallelize(Array(("a",25),("b",20),("c",15),("a",10),("c",35)))
x: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[3] at parallelize at <console>:23

val y = x.groupByKey()
y: org.apache.spark.rdd.RDD[(String, Iterable[Int])] = ShuffledRDD[4] at groupByKey at <console>:23

y.collect
res8: Array[(String, Iterable[Int])] = Array((a,CompactBuffer(25, 10)), (b,CompactBuffer(20)), (c,CompactBuffer(15, 35)))
22. Actions - reduce,collect,first,top,count,max
a)rdd1.reduce(_+_)
res0: Int = 55
b)rdd1.collect
res1: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
c)rdd1.first
res2: Int = 1
d)rdd1.top(3)
res3: Array[Int] = Array(10, 9, 8)
e)rdd1.count
res4: Long = 10
f)rdd1.max
res5: Int = 10
23. Functions- Unit, Higher Order, Anonymous, Recursive
24. scala> val rdd2 = sc.textFile("C:/Users/abhir/OneDrive/Desktop/sai kiran sa
hu.txt")
rdd2: org.apache.spark.rdd.RDD[String] = C:/Users/abhir/OneDrive/Desktop/sai kiran sahu.txt MapPartitionsRDD[1] at textFile at <console>:23

scala> rdd2.map(x=>x.split(" ")).collect
res0: Array[Array[String]] = Array(Array(sai, kiran, sahu), Array(sai, kiran, sahu), Array(sai, kiran, sahu))

scala> rdd2.flatMap(x=>x.split(" ")).collect
res1: Array[String] = Array(sai, kiran, sahu, sai, kiran, sahu, sai, kiran, sahu)

scala> rdd2.flatMap(x=>x.split(" ")).saveAsTextFile{"C:/Users/abhir/OneDrive/Desktop/output.txt"}
25.Cartesian, coalesce(decrease the no of partitions), repartition(increase/decrease the no of partitions), countByKey, countByValue
scala> val a = Array(1,2,3,4,5)
a: Array[Int] = Array(1, 2, 3, 4, 5)

scala> val rdd1 = sc.parallelize(a)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[8] at parallelize at <console>:24

scala> rdd1.collect
res4: Array[Int] = Array(1, 2, 3, 4, 5)

scala> val b = Array(6,7,8,9,10)
b: Array[Int] = Array(6, 7, 8, 9, 10)

scala> val rdd3 = sc.parallelize(b)
rdd3: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[9] at parallelize at <console>:24

scala> rdd3.collect
res5: Array[Int] = Array(6, 7, 8, 9, 10)

scala> val rdd4 = rdd1.cartesian(rdd3)
rdd4: org.apache.spark.rdd.RDD[(Int, Int)] = CartesianRDD[10] at cartesian at <console>:24

scala> rdd4.collect
res6: Array[(Int, Int)] = Array((1,6), (1,7), (1,8), (1,9), (1,10), (2,6), (2,7), (2,8), (2,9), (2,10), (3,6), (3,7), (3,8), (3,9), (3,10), (4,6), (5,6), (4,7), (5,7), (4,8), (5,8), (4,9), (4,10), (5,9), (5,10))

ii) scala> val x = sc.parallelize(Array(1,2,3,4,5),3)
x: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[11] at parallelize at <console>:23

scala> val y = x.coalesce(2)
y: org.apache.spark.rdd.RDD[Int] = CoalescedRDD[12] at coalesce at <console>:23

scala> y.collect
res7: Array[Int] = Array(1, 2, 3, 4, 5)

scala> val xOut = x.glom().collect()
xOut: Array[Array[Int]] = Array(Array(1), Array(2, 3), Array(4, 5))

scala> val yOut = y.glom().collect()
yOut: Array[Array[Int]] = Array(Array(1), Array(2, 3, 4, 5))

iii) val x = sc.parallelize(Array(1,2,3,4,5),3)
x: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[15] at parallelize at <console>:23

val y = x.repartition(5)
y: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[21] at repartition at <console>:23

scala> val xOut = x.glom().collect()
xOut: Array[Array[Int]] = Array(Array(1), Array(2, 3), Array(4, 5))

val yOut = y.glom().collect()
yOut: Array[Array[Int]] = Array(Array(3, 5), Array(), Array(), Array(), Array(1, 2, 4))

26. scala> val c = Array(2,3,4,1,2,2,3,1,1)
c: Array[Int] = Array(2, 3, 4, 1, 2, 2, 3, 1, 1)

scala> val rdd5 = sc.parallelize(c)
rdd5: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[24] at parallelize at <console>:24

scala> rdd5.countByValue
res10: scala.collection.Map[Int,Long] = Map(4 -> 1, 1 -> 3, 2 -> 3, 3 -> 2)

26.
scala> val x = sc.parallelize(Array(1,2,3,4,5),3)
x: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:23

scala> val y = x.coalesce(2)
y: org.apache.spark.rdd.RDD[Int] = CoalescedRDD[1] at coalesce at <console>:23

scala> y.partitions.length
res0: Int = 2

scala> val x = sc.parallelize(Array(1,2,3,4,5),3)
x: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[2] at parallelize at <console>:23

scala> val y = x.repartition(4)
y: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[6] at repartition at <console>:23

scala> y.partitions.length
res1: Int = 4

27. first,fold,reduce,distinct
a)scala> val rdd1 = sc.parallelize(Array(2,3,4))
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[9] at parallelize at <console>:23

scala> rdd1.reduce(_+_)
res4: Int = 9

b)scala> rdd1.first
res5: Int = 2

c)scala> rdd1.fold(1)(_+_)
res6: Int = 14

d)scala> val rdd2 = sc.parallelize(Array(1,1,2,2,3,4,5,5,3,3,2))
rdd2: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[10] at parallelize at <console>:23

scala> rdd2.distinct
res7: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[13] at distinct at <console>:24

scala> rdd2.distinct.collect()
res8: Array[Int] = Array(4, 1, 5, 2, 3)

28. GroupByKey, ReduceByKey
scala> val arr1 = List(("a",1),("b",2),("a",3),("b",1),("c",2))
arr1: List[(String, Int)] = List((a,1), (b,2), (a,3), (b,1), (c,2))

scala> val rdd1 = sc.parallelize(arr1)
rdd1: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> rdd1.groupByKey.collect()
res0: Array[(String, Iterable[Int])] = Array((a,CompactBuffer(1, 3)), (b,CompactBuffer(2, 1)), (c,CompactBuffer(2)))

scala> rdd1.reduceByKey(_+_).collect()
res1: Array[(String, Int)] = Array((a,4), (b,3), (c,2))

29. union,intersection,subtract
scala> val rdd1 = sc.parallelize(Array(1,1,2,3,4,5,1,3,4))
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[3] at parallelize at <console>:23

scala> val rdd2 = sc.parallelize(List(2,3,1,4,4,5,2,1,3))
rdd2: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[4] at parallelize at <console>:23

scala> rdd1.union(rdd2)
res2: org.apache.spark.rdd.RDD[Int] = UnionRDD[5] at union at <console>:25

scala> rdd1.union(rdd2).collect()
res3: Array[Int] = Array(1, 1, 2, 3, 4, 5, 1, 3, 4, 2, 3, 1, 4, 4, 5, 2, 1, 3)

scala> rdd1.intersection(rdd2).collect()
res4: Array[Int] = Array(4, 1, 5, 2, 3)

scala> rdd1.subtract(rdd2).collect()
res5: Array[Int] = Array()

30. sortBy
scala> val rdd1 = sc.parallelize(Array(1,1,2,3,4,5,1,3,4))
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[3] at parallelize at <console>:23

scala> rdd1.sortBy(x=>x,true).collect()
res8: Array[Int] = Array(1, 1, 1, 2, 3, 3, 4, 4, 5)

scala> rdd1.sortBy(x=>x,false).collect()
res9: Array[Int] = Array(5, 4, 4, 3, 3, 2, 1, 1, 1)

31. join
scala> val x = sc.parallelize(Array(("a",1),("b",2)))
x: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[32] at parallelize at <console>:23

scala> val y = sc.parallelize(Array(("a",3),("b",5),("a",4)))
y: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[33] at parallelize at <console>:23

scala> val z = x.join(y)
z: org.apache.spark.rdd.RDD[(String, (Int, Int))] = MapPartitionsRDD[36] at join at <console>:24

scala> val z = x.join(y).collect()
z: Array[(String, (Int, Int))] = Array((a,(1,3)), (a,(1,4)), (b,(2,5)))
```
# This is transforming to rdd -> base class-> data frame
```
Pratical-1:
scala> val rdd1 = sc.textFile("C:/Users/abhir/Downloads/xbox(in).csv")
rdd1: org.apache.spark.rdd.RDD[String] = C:/Users/abhir/Downloads/xbox(in).csv MapPartitionsRDD[1] at textFile at <console>:23

scala> rdd1.first
res0: String = ,95,2.927373,jake7870,0,95,117.5,,,,

// SQL Context entry point for working with structured data
 val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

//Import Spark SQL data types ad Row
import org.apache.spark.sql._

//define the schema using case class
case class Auction(auctionid: String, bid: Float, bidtime: Float, bidder: String, bidderrate: Integer, openbid: Float, price: Float)

// create an RDD of Auction objects
val rdd2 = rdd1.map(_.split(",")).map(p => Auction(p(0),p(1).toFloat,p(2).toFloat,p(3),p(4).toInt,p(5).toFloat,p(6).toFloat))

// change rdd to dataframe
val auction = rdd2.toDF()

// prints the output
auction.show() == rdd.collect() == select * from tablename

// describe the schema
auction.printSchema()

auction.select("bidderrate").show()
```
# Few examples of data frame using Scala
1.Creating the DataFrame in scala using 

`->  val df = scala.read.format("csv").option("header","true").option("inferSchema","true").load("file_location)` 

this is for loading the data frame from the scala 



1.View head of the matches of the table

 `IplDataSet.show(5)` 

2.Total number of matches played in ipl editions between 2008 and 2017

```
// Filter matches between 2008 and 2017 and count them
val totalMatches = df.filter(col("season").between(2008, 2017)).count()

// Print the result
println(s"Total number of matches played between 2008 and 2017: $totalMatches")
```
3.team wins count across all seasons for all teams

```
ipl.groupBy("winner").count().orderBy(desc("count")).show()
+--------------------+-----+
|              winner|count|
+--------------------+-----+
|      Mumbai Indians|  120|
| Chennai Super Kings|  106|
|Kolkata Knight Ri...|   99|
|Royal Challengers...|   91|
|     Kings XI Punjab|   88|
|    Rajasthan Royals|   81|
|    Delhi Daredevils|   67|
| Sunrisers Hyderabad|   66|
|     Deccan Chargers|   29|
|      Delhi Capitals|   19|
|       Gujarat Lions|   13|
|       Pune Warriors|   12|
|Rising Pune Super...|   10|
|Kochi Tuskers Kerala|    6|
|Rising Pune Super...|    5|
|                  NA|    4|
+--------------------+-----+
```
4.matches decision was determined by dl method for each team

```
val dl = ipl.filter("method =='DL'").groupBy("winner").count().show()
+--------------------+-----+
|              winner|count|
+--------------------+-----+
| Sunrisers Hyderabad|    2|
| Chennai Super Kings|    2|
|Kochi Tuskers Kerala|    1|
|    Rajasthan Royals|    1|
|Royal Challengers...|    3|
|Kolkata Knight Ri...|    4|
|Rising Pune Super...|    2|
|     Kings XI Punjab|    2|
|    Delhi Daredevils|    2|
+--------------------+-----+

dl: Unit = ()
```


5.max win by runs margins for all teams across all years



6.max win by wickets margins for all teams across all years

7.most player of the match award

8.Bowler with maximum wides

9.cities which have hosted the maximum nunber of matches in ipl editions btwn 2008 and 2017 

10.venue which have hosted the maximum number of the matches in ipl editions btwn 2008 and 2017 

11.most caught and bowled by a bowler 

12.most caught by a player by a bowler

13. bowler with  maximum wickets





<!--- Eraser file: https://app.eraser.io/workspace/0dKIJwDuDzX7EJDEmgWJ --->