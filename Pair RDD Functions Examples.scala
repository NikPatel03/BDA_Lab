// Databricks notebook source
//In this section we will learn PairRDDFunctions class and Spark Pair RDD transformations & action functions with scala examples.

//Pair RDD Functions Examples
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
   .appName("SparkByExample")
   .master("local")
   .getOrCreate()

/* This snippet creates a pair RDD by splitting by space on every element in an RDD, flatten it to form a single word string on each element in RDD and finally assigns an integer “1” to every word.*/

val rdd = spark.sparkContext.parallelize(
      List("Germany India USA","USA India Russia","India Brazil Canada China")
    )
 val wordsRdd = rdd.flatMap(_.split(" "))
 val pairRDD = wordsRdd.map(f=>(f,1))
 pairRDD.collect
//pairRDD.foreach(println)

// COMMAND ----------

// distinct – Returns distinct keys.
pairRDD.distinct.collect

// COMMAND ----------

//sortByKey – Transformation returns an RDD after sorting by key
println("Sort by Key ==>")
    val sortRDD = pairRDD.sortByKey()
    sortRDD.collect

// COMMAND ----------

// reduceByKey – Transformation returns an RDD after adding value for each key.
 println("Reduce by Key ==>")
    val wordCount = pairRDD.reduceByKey((a,b)=>a+b)
    wordCount.collect
//Result RDD contains unique keys.
//This reduces the key by summing the values. Yields below output.

// COMMAND ----------

/* aggregateByKey – Transformation same as reduceByKey
In our example, this is similar to reduceByKey but uses a different approach.*/

    def param1= (accu:Int,v:Int) => accu + v
    def param2= (accu1:Int,accu2:Int) => accu1 + accu2
    println("Aggregate by Key ==> wordcount")
    val wordCount2 = pairRDD.aggregateByKey(0)(param1,param2)
    //wordCount2.foreach(println)
    wordCount2.collect
//This example yields the same output as reduceByKey example.

// COMMAND ----------

//keys – Return RDD[K] with all keys in an dataset
 println("Keys ==>")
    wordCount2.keys.collect

// COMMAND ----------

//values – return RDD[V] with all values in an dataset
println("Values ==>")
    wordCount2.values.collect

// COMMAND ----------

// count – This is an action function and returns a count of a dataset
println("Count :"+wordCount2.count())

// COMMAND ----------

/* collectAsMap – This is an action function and returns Map to the master for retrieving all data from a dataset.*/
println("collectAsMap ==>")
    pairRDD.collectAsMap().foreach(println)

// COMMAND ----------

/* Spark repartition() vs coalesce() – repartition() is used to increase or decrease the RDD, DataFrame, Dataset partitions whereas the coalesce() is used to only decrease the number of partitions in an efficient way.
In this section, you will learn what is Spark repartition() and coalesce() methods? and the difference between repartition vs coalesce with Scala examples.

RDD Partition
RDD repartition
RDD coalesce
DataFrame Partition
DataFrame repartition
DataFrame coalesce
One important point to note is, Spark repartition() and coalesce() are very expensive operations as they shuffle the data across many partitions hence try to minimize repartition as much as possible.

1. Spark RDD repartition() vs coalesce()

In RDD, you can create parallelism at the time of the creation of an RDD using parallelize(), textFile() and wholeTextFiles(). You can download the test.txt file used in this example from GitHub.*/

// COMMAND ----------

val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val rdd = spark.sparkContext.parallelize(Range(0,20))
  println("From local[5]"+rdd.partitions.size)

  val rdd1 = spark.sparkContext.parallelize(Range(0,25), 6)
  println("parallelize : "+rdd1.partitions.size)

  val rddFromFile = spark.sparkContext.textFile("dbfs:/FileStore/shared_uploads/samir_mece@yahoo.com/test.txt",10)
  println("TextFile : "+rddFromFile.partitions.size)

// COMMAND ----------

// spark.sparkContext.parallelize(Range(0,20),6) distributes RDD into 6 partitions and the data is distributed as below.
rdd1.saveAsTextFile("/tmp/partition")
/* Writes 6 part files, one for each partition
Partition 1 : 0 1 2
Partition 2 : 3 4 5
Partition 3 : 6 7 8 9
Partition 4 : 10 11 12
Partition 5 : 13 14 15
Partition 6 : 16 17 18 19
*/

// COMMAND ----------

/* 1.1 RDD repartition()
Spark RDD repartition() method is used to increase or decrease the partitions. The below example decreases the partitions from 10 to 4 by moving data from all partitions.*/
val rdd2 = rdd1.repartition(4)
  println("Repartition size : "+rdd2.partitions.size)
  rdd2.saveAsTextFile("/tmp/repartition")
/* This yields output Repartition size : 4 and the repartition re-distributes the data(as shown below) from all partitions which is full shuffle leading to very expensive operation when dealing with billions and trillions of data.
Partition 1 : 1 6 10 15 19
Partition 2 : 2 3 7 11 16
Partition 3 : 4 8 12 13 17
Partition 4 : 0 5 9 14 18
*/

// COMMAND ----------

/* 1.2 RDD coalesce()
Spark RDD coalesce() is used only to reduce the number of partitions. This is optimized or improved version of repartition() where the movement of the data across the partitions is lower using coalesce.*/
val rdd3 = rdd1.coalesce(4)
  println("Repartition size : "+rdd3.partitions.size)
  rdd3.saveAsTextFile("/tmp/coalesce")

/* If you compared the below output with section 1, you will notice partition 3 has been moved to 2 and Partition 6 has moved to 5, resulting data movement from just 2 partitions.

Partition 1 : 0 1 2
Partition 2 : 3 4 5 6 7 8 9
Partition 4 : 10 11 12 
Partition 5 : 13 14 15 16 17 18 19
*/

// COMMAND ----------

/* 2. Spark DataFrame repartition() vs coalesce()
Unlike RDD, you can’t specify the partition/parallelism while creating DataFrame. DataFrame or Dataset by default uses the methods specified in Section 1 to determine the default partition and splits the data for parallelism.*/

  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    .getOrCreate()

 val df = spark.range(0,20)
 println(df.rdd.partitions.length)

 df.write.mode(SaveMode.Overwrite)csv("partition.csv")
/* The above example creates 5 partitions as specified in master("local[5]") and the data is distributed across all these 5 partitions.
Partition 1 : 0 1 2 3
Partition 2 : 4 5 6 7
Partition 3 : 8 9 10 11
Partition 4 : 12 13 14 15
Partition 5 : 16 17 18 19 */


// COMMAND ----------

/* 2.1 DataFrame repartition()
Similar to RDD, the Spark DataFrame repartition() method is used to increase or decrease the partitions. The below example increases the partitions from 5 to 6 by moving data from all partitions.
*/
val df2 = df.repartition(6)
println(df2.rdd.partitions.length)



// COMMAND ----------

/* Just increasing 1 partition results data movements from all partitions.
Partition 1 : 14 1 5
Partition 2 : 4 16 15
Partition 3 : 8 3 18
Partition 4 : 12 2 19
Partition 5 : 6 17 7 0
Partition 6 : 9 10 11 13
And, even decreasing the partitions also results in moving data from all partitions. hence when you wanted to decrease the partition recommendation is to use coalesce() */

// COMMAND ----------

/* 2.2 DataFrame coalesce()
Spark DataFrame coalesce() is used only to decrease the number of partitions. This is an optimized or improved version of repartition() where the movement of the data across the partitions is fewer using coalesce. */
val df3 = df.coalesce(2)
 println(df3.rdd.partitions.length)

/*This yields output 2 and the resultant partition looks like

Partition 1 : 0 1 2 3 8 9 10 11
Partition 2 : 4 5 6 7 12 13 14 15 16 17 18 19

Since we are reducing 5 to 2 partitions, the data movement happens only from 3 partitions and it moves to remain 2 partitions.*/

// COMMAND ----------

/*Default Shuffle Partition
Calling groupBy(), union(), join() and similar functions on DataFrame results in shuffling data between multiple executors and even machines and finally repartitions data into 200 partitions by default. Spark default defines shuffling partition to 200 using spark.sql.shuffle.partitions configuration.*/
 val df4 = df.groupBy("id").count()
 println(df4.rdd.getNumPartitions)

// COMMAND ----------

/*In this Spark repartition and coalesce section, you have learned how to create an RDD with partition, repartition the RDD & DataFrame using repartition() and coalesce() methods, and learned the difference between repartition and coalesce. */

// COMMAND ----------

/* Spark SQL Shuffle Partitions
The Spark SQL shuffle is a mechanism for redistributing or re-partitioning data so that the data grouped differently across partitions, based on your data size you may need to reduce or increase the number of partitions of RDD/DataFrame using spark.sql.shuffle.partitions configuration or through code.

Spark shuffle is a very expensive operation as it moves the data between executors or even between worker nodes in a cluster so try to avoid it when possible. When you have a performance issue on Spark jobs, you should look at the Spark transformations that involve shuffling.

In this tutorial, you will learn what triggers the shuffle on RDD and DataFrame transformations using scala examples. The same approach also can be used with PySpark (Spark with Python)

What is Spark Shuffle?
Shuffling is a mechanism Spark uses to redistribute the data across different executors and even across machines. Spark shuffling triggers for transformation operations like gropByKey(), reducebyKey(), join(), union(), groupBy() e.t.c
Spark Shuffle is an expensive operation since it involves the following

  Disk I/O
  Involves data serialization and deserialization
  Network I/O
When creating an RDD, Spark doesn’t necessarily store the data for all keys in a partition since at the time of creation there is no way we can set the key for data set.

Hence, when we run the reduceByKey() operation to aggregate the data on keys, Spark does the following.

Spark first runs map tasks on all partitions which groups all values for a single key.
The results of the map tasks are kept in memory.
When results do not fit in memory, Spark stores the data into a disk.
Spark shuffles the mapped data across partitions, some times it also stores the shuffled data into a disk for reuse when it needs to recalculate.
Run the garbage collection
Finally runs reduce tasks on each partition based on key.

Spark RDD Shuffle
Spark RDD triggers shuffle for several operations like repartition(), coalesce(),  groupByKey(),  reduceByKey(), cogroup() and join() but not countByKey() */

val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    .getOrCreate()

val sc = spark.sparkContext

val rdd:RDD[String] = sc.textFile("dbfs:/FileStore/shared_uploads/samir_mece@yahoo.com/test.txt")

println("RDD Parition Count :"+rdd.getNumPartitions)
val rdd2 = rdd.flatMap(f=>f.split(" "))
  .map(m=>(m,1))

//ReduceBy transformation
val rdd5 = rdd2.reduceByKey(_ + _)

println("RDD Parition Count :"+rdd5.getNumPartitions)
/*
Output
RDD Parition Count : 2
RDD Parition Count : 2


Both getNumPartitions from the above examples return the same number of partitions. Though reduceByKey() triggers data shuffle, it doesn’t change the partition count as RDD’s inherit the partition size from parent RDD.

You may get partition count different based on your setup and how Spark creates partitions.*/

// COMMAND ----------

/*Spark SQL DataFrame Shuffle
Unlike RDD, Spark SQL DataFrame API increases the partitions when the transformation operation performs shuffling. DataFrame operations that trigger shufflings are join(), union() and all aggregate functions.*/


import spark.implicits._

val simpleData = Seq(("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )
val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")

val df2 = df.groupBy("state").count()

println(df2.rdd.getNumPartitions)

/*This outputs the partition count as 200.
Spark Default Shuffle Partition
DataFrame increases the partition number to 200 automatically when Spark operation performs data shuffling (join(), union(), aggregation functions). This default shuffle partition number comes from Spark SQL configuration <strong>spark.sql.shuffle.partitions</strong> which is by default set to <strong>200</strong>.

You can change this default shuffle partition value using conf method of the SparkSession object or using Spark Submit Command Configurations. */
spark.conf.set("spark.sql.shuffle.partitions",100)
println(df.groupBy("_c0").count().rdd.partitions.length)
/*Shuffle partition size
Based on your dataset size, number of cores, and memory, Spark shuffling can benefit or harm your jobs. When you dealing with less amount of data, you should typically reduce the shuffle partitions otherwise you will end up with many partitioned files with a fewer number of records in each partition. which results in running many tasks with lesser data to process.

On other hand, when you have too much of data and having less number of partitions results in fewer longer running tasks and some times you may also get out of memory error.

Getting a right size of the shuffle partition is always tricky and takes many runs with different value to achieve the optimized number. This is one of the key property to look for when you have performance issues on Spark jobs.
Conclusion
In this article, you have learned what is Spark SQL shuffle, how some Spark operation triggers re-partition the data, how to change the default spark shuffle partition, and finally how to get right partition size.*/

// COMMAND ----------


