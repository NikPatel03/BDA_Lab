// Databricks notebook source
sc.setLogLevel("ERROR")

// COMMAND ----------

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// COMMAND ----------

val a = sc.parallelize(1 to 10)

// COMMAND ----------

a.collect

// COMMAND ----------

val b = a.map(x=>(x,x+1))

// COMMAND ----------

b.collect

// COMMAND ----------

val df = b.toDF("First","Second")

// COMMAND ----------

df.show

// COMMAND ----------

val a = List(("Tom",5),("Jerry",2),("Donald",7))

// COMMAND ----------

val df = a.toDF("Name","Age")

// COMMAND ----------

df.show

// COMMAND ----------

val a =Seq(("Tom",5),("Jerry",2),("Donald",7))

// COMMAND ----------

val df = a.toDF("Name","Age")

// COMMAND ----------

df.show

// COMMAND ----------

df.registerTempTable("Cartoon")

// COMMAND ----------

df.createOrReplaceTempView("Cartoon")

// COMMAND ----------

sqlContext.sql("select * from Cartoon where Name ='Tom'").show

// COMMAND ----------

sqlContext.sql("select * from Cartoon").show

// COMMAND ----------

sqlContext.sql("select count(*) from Cartoon").show

// COMMAND ----------

// Question: To create a JSON file, upload it on DBFS and perform the following operations on it

//{"id":"1201","name":"Satish","Age":"25"}
//{"id":"1202","name":"Krishna","Age":"28"}
//{"id":"1203","name":"Amith","Age":"39"}
//{"id":"1204","name":"Javed","Age":"23"}
//{"id":"1205","name":"Pruthvi","Age":"23"}

//printSchema()
//select query with all names
//filter and identify age >23
//groupBy Age count it and show it

// COMMAND ----------

val df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/samir_mece@yahoo.com/emp-1.json")

// COMMAND ----------

df1.show

// COMMAND ----------

df1.printSchema()

// COMMAND ----------

df1.select("name","Age").show()

// COMMAND ----------

df1.createOrReplaceTempView("employee")

// COMMAND ----------

sqlContext.sql("select name from employee").show

// COMMAND ----------

df1.filter("age > '23'").show()
// alternate way of doing the same thing
df1.filter(df1("age") > 23).show()

// COMMAND ----------

df1.groupBy("age").count().show

// COMMAND ----------

1

// COMMAND ----------

1+2

// COMMAND ----------

val rdda = sc.parallelize(1 to 1000)

// COMMAND ----------

rdda.collect

// COMMAND ----------

val rddb = sc.parallelize(List("BMW","Mercedes","Toyota","Audi"))

// COMMAND ----------

rddb.collect()

// COMMAND ----------

rdda.partitions.length

// COMMAND ----------

rddb.partitions.length

// COMMAND ----------

val rdda = sc.parallelize(1 to 1000,10)

// COMMAND ----------

rdda.partitions.length

// COMMAND ----------

rdda.count

// COMMAND ----------

rdda.collect()

// COMMAND ----------

rdda.first

// COMMAND ----------

rdda.take(10)

// COMMAND ----------

rdda.count()

// COMMAND ----------

rdda.saveAsTextFile("dbfs:/FileStore/shared_uploads/samir_mece@yahoo.com/parallelEX.txt")

// COMMAND ----------

val rddread = sc.textFile("dbfs:/FileStore/shared_uploads/samir_mece@yahoo.com/parallelEX.txt")

// COMMAND ----------

rddread.count()

// COMMAND ----------

rddread.collect()

// COMMAND ----------

/*Assignment

Prepare a text file which contains
1. Paradigms of Aritificial Intelligence Programming: Case Studies in Common Lisp
2. Code: The Hidden Language of Computer Hardware and Software
3. An introduction to algorithms
4. Aritificial Intelligence: A modern approach
5. ON LISP
6. ANSI Common LISP
7. LISP in small pieces
8. The little lisper
9. The seasoned schemer 
upload it as book.txt

dbfs:/FileStore/shared_uploads/samir_mece@yahoo.com/book.txt
*/

// COMMAND ----------

book.collect

// COMMAND ----------

val book = sc.textFile("dbfs:/FileStore/shared_uploads/samir_mece@yahoo.com/book.txt")

// COMMAND ----------

book.collect

// COMMAND ----------

val a = book.flatMap(x=>x.split(" ")).collect()

// COMMAND ----------

val b = a.map( y => (y,1))

// COMMAND ----------

val a = book.flatMap(x => x.split(" ")).map(y => (y,1)).reduceByKey((x,y) => (x+y)).collect

// COMMAND ----------

val a = book.flatMap(x => x.split(" ")).map(y => (y,1)).reduceByKey((x,y) => (x+y)).sortBy(_._1,false).collect

// COMMAND ----------

val a = book.flatMap(x => x.split(" ")).map(y => (y,1)).reduceByKey((x,y) => (x+y)).sortBy(_._2,false).collect

// COMMAND ----------

val a = book.flatMap(x => x.split(" ")).map(y => (y,1)).reduceByKey((x,y) => (x+y)).sortBy(_._2,false).take(1)

// COMMAND ----------

val a = book.flatMap(x => x.split(" ")).map(y => (y,1)).reduceByKey((x,y) => (x+y)).sortBy(_._2,false).filter(_._2 > 1).collect

// COMMAND ----------


