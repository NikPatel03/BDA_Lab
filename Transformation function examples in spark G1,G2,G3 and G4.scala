// Databricks notebook source

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDParallelize {

  def main(args: Array[String]): Unit = {
      val spark:SparkSession = SparkSession.builder().master("local[1]")
          .appName("SparkByExamples.com")
          .getOrCreate()
      val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
      val rddCollect:Array[Int] = rdd.collect()
      println("Number of Partitions: "+rdd.getNumPartitions)
      println("Action: First element: "+rdd.first())
      println("Action: RDD converted to Array[Int] : ")
      rddCollect.foreach(println)
  }
}

// COMMAND ----------

val rdda = sc.parallelize(List(1,2,3,4,5))
      val rddb = rdda.collect
      println("Number of Partitions: "+rdda.getNumPartitions)
      println("Action: First element: "+rdda.first())
      rdda.foreach(println)

// COMMAND ----------

val rdda = sc.parallelize(List("mumbai","delhi","Chennai","Kolkatta"))

// COMMAND ----------

val rddb = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))

// COMMAND ----------


val rddc= sc.parallelize(Seq.empty[String])


// COMMAND ----------

rdda.collect

// COMMAND ----------

val b = rdda.map(x => (x,1))

// COMMAND ----------

b.collect

// COMMAND ----------

//Samething using shorthand notation
val b = rdda.map((_,1))

// COMMAND ----------

b.collect

// COMMAND ----------

val b=rdda.map(x=>(x,x.length))

// COMMAND ----------

b.collect

// COMMAND ----------

val a = sc.parallelize(List(1,2,3,4,5)).map(x=>List(x,x,x)).collect

// COMMAND ----------

val a = sc.parallelize(List(1,2,3,4,5)).flatMap(x=>List(x,x,x)).collect

// COMMAND ----------

// applying filter on rdda city  data
val rdda = sc.parallelize(List("mumbai","delhi","Chennai","Kolkatta")).filter(_.equals("mumbai")).collect

// COMMAND ----------

// applying filter which contains e in the data
val rdda = sc.parallelize(List("mumbai","delhi","Chennai","Kolkatta")).filter(_.contains("e")).collect

// COMMAND ----------

// create an RDD with city,count
val a = sc.parallelize(List(("mumbai",4000),("Delhi",2000),("Chennai",1000),("Kolkatta",7000)))

// COMMAND ----------

// perform filter operation where value equals 4000
val a = sc.parallelize(List(("mumbai",4000),("Delhi",2000),("Chennai",1000),("Kolkatta",7000))).filter(_._2.equals(4000)).collect

// COMMAND ----------

// perform filter operation where value is greater than 3000
val a = sc.parallelize(List(("mumbai",4000),("Delhi",2000),("Chennai",1000),("Kolkatta",7000))).filter(_._2 >3000).collect

// COMMAND ----------

// perform filter which starts with C

val a = sc.parallelize(List(("mumbai",4000),("Delhi",2000),("Chennai",1000),("Kolkatta",7000))).filter(a=> a._1.startsWith("C")).collect

// Perform filter by range between 3000,9000

val b = sc.parallelize(List((4000,"mumbai"),(2000,"Delhi"),(1000,"Chennai"),(7000,"Kolkatta"))).filterByRange(3000,9000).collect



// COMMAND ----------

//sample (false/true, fraction, seed)
// false - can not have repeated values
//True - will have repeated values
// fraction - 0 to 1. no. of samples in o/p
//seed - result will  be same if the seed is kept same

// COMMAND ----------

val a = sc.parallelize(1 to 100)

// COMMAND ----------

a.sample(false,.2,5).collect

// COMMAND ----------

a.sample(false,.2,5).collect

// COMMAND ----------

a.sample(false,.2,6).collect

// COMMAND ----------

a.sample(true,.2,5).collect

// COMMAND ----------

a.sample(true,.2,5).collect

// COMMAND ----------

a.sample(false,.2).collect

// COMMAND ----------

a.sample(false,.2).collect

// COMMAND ----------

a.sample(false,1,5).collect

// COMMAND ----------

val a = sc.parallelize(List(1,2,1,1,1,2))

// COMMAND ----------

a.sample(true,.4,5).collect

// COMMAND ----------

a.sample(false,.4,5).collect

// COMMAND ----------

val a = sc.parallelize(1 to 7)

// COMMAND ----------

val b = sc.parallelize(5 to 10)

// COMMAND ----------

a.union(b).collect

// COMMAND ----------

a.intersection(b).collect

// COMMAND ----------

a.union(b).distinct.collect

// COMMAND ----------

val a = sc.parallelize(1 to 9,3)

// COMMAND ----------

a.mapPartitions(x=>List(x.next).iterator).collect

// COMMAND ----------

val a = sc.parallelize(1 to 9,4)

// COMMAND ----------

a.mapPartitions(x=>List(x.next).iterator).collect

// COMMAND ----------

def practfunct(index:Int, iter:Iterator[(Int)]) : Iterator[String] = {
  iter.toList.map(x=> "[index :" +index + ", val : "+x + "]").iterator
}

// COMMAND ----------

val a = sc.parallelize(List(1,2,3,4,5,6),2)

// COMMAND ----------

a.collect

// COMMAND ----------

a.mapPartitionsWithIndex(practfunct).collect

// COMMAND ----------

val a = sc.parallelize(List(1,2,3,4,5,6),3)

// COMMAND ----------

a.mapPartitionsWithIndex(practfunct).collect

// COMMAND ----------

val broadcastVar = sc.broadcast(Array(1, 2, 3))

// COMMAND ----------

broadcastVar.value

// COMMAND ----------

/* Spark RDD Broadcast variable example
Below is a very simple example of how to use broadcast variables on RDD. This example defines commonly used data (country and states) in a Map variable and distributes the variable using SparkContext.broadcast() and then use these variables on RDD map() transformation.*/

import org.apache.spark.sql.SparkSession

/*object RDDBroadcast extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExamples.com")
    .master("local")
    .getOrCreate()
*/
  val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
  val countries = Map(("USA","United States of America"),("IN","India"))

  val broadcastStates = spark.sparkContext.broadcast(states)
  val broadcastCountries = spark.sparkContext.broadcast(countries)

  val data = Seq(("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  )

  val rdd = spark.sparkContext.parallelize(data)
  //rdd.collect
  val rdd2 = rdd.map(f=>{
    val country = f._3
    val state = f._4
    val fullCountry = broadcastCountries.value.get(country).get
    val fullState = broadcastStates.value.get(state).get
    (f._1,f._2,fullCountry,fullState)
  })

  println(rdd2.collect().mkString("\n"))

//}


// COMMAND ----------

/*Spark DataFrame Broadcast variable example
Below is an example of how to use broadcast variables on DataFrame. similar to above RDD example, This defines commonly used data (country and states) in a Map variable and distributes the variable using SparkContext.broadcast() and then use these variables on DataFrame map() transformation.
import org.apache.spark.sql.SparkSession */

/*object BroadcastExample extends App{

  val spark = SparkSession.builder()
    .appName("SparkByExamples.com")
    .master("local")
    .getOrCreate()
*/
  val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
  val countries = Map(("USA","United States of America"),("IN","India"))

  val broadcastStates = spark.sparkContext.broadcast(states)
  val broadcastCountries = spark.sparkContext.broadcast(countries)

  val data = Seq(("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  )

  val columns = Seq("firstname","lastname","country","state")
  import spark.sqlContext.implicits._
  val df = data.toDF(columns:_*)

  val df2 = df.map(row=>{
    val country = row.getString(2)
    val state = row.getString(3)

    val fullCountry = broadcastCountries.value.get(country).get
    val fullState = broadcastStates.value.get(state).get
    (row.getString(0),row.getString(1),fullCountry,fullState)
  }).toDF(columns:_*)

  df2.show(false)
//}


// COMMAND ----------

val accum = sc.longAccumulator("SumAccumulator")

// COMMAND ----------

sc.parallelize(Array(1, 2, 3)).foreach(x => accum.add(x))

// COMMAND ----------

accum.value

// COMMAND ----------

val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")
  
  val rdd = spark.sparkContext.parallelize(Array(1, 2, 3))

  rdd.foreach(x => longAcc.add(x))
  println(longAcc.value)

// COMMAND ----------


  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))
  
  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))


// COMMAND ----------


  //aggregate
  def param0= (accu:Int, v:Int) => accu + v
  def param1= (accu1:Int,accu2:Int) => accu1 + accu2
  println("aggregate : "+listRdd.aggregate(0)(param0,param1))
  //Output: aggregate : 20

  //aggregate
  def param3= (accu:Int, v:(String,Int)) => accu + v._2
  def param4= (accu1:Int,accu2:Int) => accu1 + accu2
  println("aggregate : "+inputRDD.aggregate(0)(param3,param4))
  //Output: aggregate : 181


// COMMAND ----------

/* treeAggregate – action
treeAggregate() – Aggregates the elements of this RDD in a multi-level tree pattern. The output of this function will be similar to the aggregate function.*/

  //treeAggregate. This is similar to aggregate
  def param8= (accu:Int, v:Int) => accu + v
  def param9= (accu1:Int,accu2:Int) => accu1 + accu2
  println("treeAggregate : "+listRdd.treeAggregate(0)(param8,param9))
  //Output: treeAggregate : 20


// COMMAND ----------

/* fold – action
fold() – Aggregate the elements of each partition, and then the results for all the partitions.*/

  //fold
  println("fold :  "+listRdd.fold(0){ (acc,v) =>
    val sum = acc+v
    sum
  })
  //Output: fold :  20

  println("fold :  "+inputRDD.fold(("Total",0)){(acc:(String,Int),v:(String,Int))=>
    val sum = acc._2 + v._2
    ("Total",sum)
  })
  //Output: fold :  (Total,181)


// COMMAND ----------

/* reduce
reduce() – Reduces the elements of the dataset using the specified binary operator.*/

  //reduce
  println("reduce : "+listRdd.reduce(_ + _))
  //Output: reduce : 20
  println("reduce alternate : "+listRdd.reduce((x, y) => x + y))
  //Output: reduce alternate : 20
  println("reduce : "+inputRDD.reduce((x, y) => ("Total",x._2 + y._2)))
  //Output: reduce : (Total,181)


// COMMAND ----------

/*treeReduce
treeReduce() – Reduces the elements of this RDD in a multi-level tree pattern.*/

  //treeReduce. This is similar to reduce
  println("treeReduce : "+listRdd.treeReduce(_ + _))
  //Output: treeReduce : 20


// COMMAND ----------

/* collect
collect() -Return the complete dataset as an Array.*/

  //Collect
  val data:Array[Int] = listRdd.collect()
  data.foreach(println)


// COMMAND ----------

/* count, countApprox, countApproxDistinct
count() – Return the count of elements in the dataset.

countApprox() – Return approximate count of elements in the dataset, this method returns incomplete when execution time meets timeout.

countApproxDistinct() – Return an approximate number of distinct elements in the dataset.*/


  //count, countApprox, countApproxDistinct
  println("Count : "+listRdd.count)
  //Output: Count : 20
  println("countApprox : "+listRdd.countApprox(1200))
  //Output: countApprox : (final: [7.000, 7.000])
  println("countApproxDistinct : "+listRdd.countApproxDistinct())
  //Output: countApproxDistinct : 5
  println("countApproxDistinct : "+inputRDD.countApproxDistinct())
  //Output: countApproxDistinct : 5


// COMMAND ----------

/* countByValue, countByValueApprox
countByValue() – Return Map[T,Long] key representing each unique value in dataset and value represents count each value present.

countByValueApprox() – Same as countByValue() but returns approximate result.*/

  //countByValue, countByValueApprox
  println("countByValue :  "+listRdd.countByValue())
  //Output: countByValue :  Map(5 -> 1, 1 -> 1, 2 -> 2, 3 -> 2, 4 -> 1)
  //println(listRdd.countByValueApprox())


// COMMAND ----------

/* first
first() – Return the first element in the dataset.*/
//first
  println("first :  "+listRdd.first())
  //Output: first :  1
  println("first :  "+inputRDD.first())
  //Output: first :  (Z,1)

// COMMAND ----------

/* top
top() – Return top n elements from the dataset.

Note: Use this method only when the resulting array is small, as all the data is loaded into the driver’s memory.*/

  //top
  println("top : "+listRdd.top(2).mkString(","))
  //Output: take : 5,4
  println("top : "+inputRDD.top(2).mkString(","))
  //Output: take : (Z,1),(C,40)


// COMMAND ----------

/* min
min() – Return the minimum value from the dataset.*/

  //min
  println("min :  "+listRdd.min())
  //Output: min :  1
  println("min :  "+inputRDD.min())
  //Output: min :  (A,20)


// COMMAND ----------

/* max
max() – Return the maximum value from the dataset.*/

  //max
  println("max :  "+listRdd.max())
  //Output: max :  5
  println("max :  "+inputRDD.max())
  //Output: max :  (Z,1)


// COMMAND ----------

/* take, takeOrdered, takeSample
take() – Return the first num elements of the dataset.

takeOrdered() – Return the first num (smallest) elements from the dataset and this is the opposite of the take() action.
Note: Use this method only when the resulting array is small, as all the data is loaded into the driver’s memory.

takeSample() – Return the subset of the dataset in an Array.
Note: Use this method only when the resulting array is small, as all the data is loaded into the driver’s memory.*/

  //take, takeOrdered, takeSample
  println("take : "+listRdd.take(2).mkString(","))
  //Output: take : 1,2
  println("takeOrdered : "+ listRdd.takeOrdered(2).mkString(","))
  //Output: takeOrdered : 1,2
  //println("take : "+listRdd.takeSample())


// COMMAND ----------

//Actions – Complete example

package com.sparkbyexamples.spark.rdd

import com.sparkbyexamples.spark.rdd.OperationOnPairRDDComplex.kv
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object RDDActions extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))

  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

  //Collect
  val data:Array[Int] = listRdd.collect()
  data.foreach(println)

  //aggregate
  def param0= (accu:Int, v:Int) => accu + v
  def param1= (accu1:Int,accu2:Int) => accu1 + accu2
  println("aggregate : "+listRdd.aggregate(0)(param0,param1))
  //Output: aggregate : 20

  //aggregate
  def param3= (accu:Int, v:(String,Int)) => accu + v._2
  def param4= (accu1:Int,accu2:Int) => accu1 + accu2
  println("aggregate : "+inputRDD.aggregate(0)(param3,param4))
  //Output: aggregate : 20

  //treeAggregate. This is similar to aggregate
  def param8= (accu:Int, v:Int) => accu + v
  def param9= (accu1:Int,accu2:Int) => accu1 + accu2
  println("treeAggregate : "+listRdd.treeAggregate(0)(param8,param9))
  //Output: treeAggregate : 20

  //fold
  println("fold :  "+listRdd.fold(0){ (acc,v) =>
    val sum = acc+v
    sum
  })
  //Output: fold :  20

  println("fold :  "+inputRDD.fold(("Total",0)){(acc:(String,Int),v:(String,Int))=>
    val sum = acc._2 + v._2
    ("Total",sum)
  })
  //Output: fold :  (Total,181)

  //reduce
  println("reduce : "+listRdd.reduce(_ + _))
  //Output: reduce : 20
  println("reduce alternate : "+listRdd.reduce((x, y) => x + y))
  //Output: reduce alternate : 20
  println("reduce : "+inputRDD.reduce((x, y) => ("Total",x._2 + y._2)))
  //Output: reduce : (Total,181)

  //treeReduce. This is similar to reduce
  println("treeReduce : "+listRdd.treeReduce(_ + _))
  //Output: treeReduce : 20

  //count, countApprox, countApproxDistinct
  println("Count : "+listRdd.count)
  //Output: Count : 20
  println("countApprox : "+listRdd.countApprox(1200))
  //Output: countApprox : (final: [7.000, 7.000])
  println("countApproxDistinct : "+listRdd.countApproxDistinct())
  //Output: countApproxDistinct : 5
  println("countApproxDistinct : "+inputRDD.countApproxDistinct())
  //Output: countApproxDistinct : 5

  //countByValue, countByValueApprox
  println("countByValue :  "+listRdd.countByValue())
  //Output: countByValue :  Map(5 -> 1, 1 -> 1, 2 -> 2, 3 -> 2, 4 -> 1)
  //println(listRdd.countByValueApprox())

  //first
  println("first :  "+listRdd.first())
  //Output: first :  1
  println("first :  "+inputRDD.first())
  //Output: first :  (Z,1)

  //top
  println("top : "+listRdd.top(2).mkString(","))
  //Output: take : 5,4
  println("top : "+inputRDD.top(2).mkString(","))
  //Output: take : (Z,1),(C,40)

  //min
  println("min :  "+listRdd.min())
  //Output: min :  1
  println("min :  "+inputRDD.min())
  //Output: min :  (A,20)

  //max
  println("max :  "+listRdd.max())
  //Output: max :  5
  println("max :  "+inputRDD.max())
  //Output: max :  (Z,1)

  //take, takeOrdered, takeSample
  println("take : "+listRdd.take(2).mkString(","))
  //Output: take : 1,2
  println("takeOrdered : "+ listRdd.takeOrdered(2).mkString(","))
  //Output: takeOrdered : 1,2
  //println("take : "+listRdd.takeSample())

  //toLocalIterator
  //listRdd.toLocalIterator.foreach(println)
  //Output:

}
/* Conclusion:
RDD actions are operations that return non-RDD values, since RDD’s are lazy they do not execute the transformation functions until we call actions. hence, all these functions trigger the transformations to execute and finally returns the value of the action functions to the driver program. and In this section, you have also learned several RDD functions usage and examples in scala language.*/

// COMMAND ----------


 val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  import spark.implicits._
  val columns = Seq("Seqno","Quote")
  val data = Seq(("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."))
  val df = data.toDF(columns:_*)

  val dfCache = df.cache()
  dfCache.show(false)


// COMMAND ----------


