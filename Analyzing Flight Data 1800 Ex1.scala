// Databricks notebook source
// MAGIC %scala
// MAGIC // load 2008.csv in specific folder and run from that place spark-shell
// MAGIC import org.apache.spark.rdd.RDD
// MAGIC import org.apache.spark.sql.SparkSession
// MAGIC sc.setLogLevel("ERROR")

// COMMAND ----------

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// COMMAND ----------

val flights = 
sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("delimiter",",").load("dbfs:/FileStore/shared_uploads/samir_mece@yahoo.com/1800.csv")


// COMMAND ----------

val flights = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("delimiter",",").load("dbfs:/FileStore/shared_uploads/samir_mece@yahoo.com/1800.csv")
def parseLine(line:String)= {
   val fliends = line.split(",")
   val place = fliends(0)
   val tempCat = fliends(2)
   val temp = fliends(3).toFloat/10.0
   (place, tempCat, temp)
  
}
val lines = sc.textFile("dbfs:/FileStore/shared_uploads/samir_mece@yahoo.com/1800.csv")
lines.collect

// COMMAND ----------

//take only required fields by using function
val linesA = lines.map(parseLine)

// COMMAND ----------

linesA.collect

// COMMAND ----------

//Take only minimum
val linesB = linesA.filter(x => x._2 =="TMIN")

// COMMAND ----------

linesB.collect

// COMMAND ----------

val linesC = linesB.map(x=> (x._1, x._3))

// COMMAND ----------

linesC.collect

// COMMAND ----------

//import maths library
import scala.math.min


// COMMAND ----------

val result = linesC.reduceByKey((x,y)=> min(x,y))

// COMMAND ----------

result.collect

// COMMAND ----------


