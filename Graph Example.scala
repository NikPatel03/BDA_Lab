// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Overview
// MAGIC 
// MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
// MAGIC 
// MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

// COMMAND ----------

// MAGIC %python
// MAGIC # File location and type
// MAGIC file_location = "/FileStore/tables/vertex-1.csv"
// MAGIC file_type = "csv"
// MAGIC 
// MAGIC # CSV options
// MAGIC infer_schema = "false"
// MAGIC first_row_is_header = "false"
// MAGIC delimiter = ","
// MAGIC 
// MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
// MAGIC df = spark.read.format(file_type) \
// MAGIC   .option("inferSchema", infer_schema) \
// MAGIC   .option("header", first_row_is_header) \
// MAGIC   .option("sep", delimiter) \
// MAGIC   .load(file_location)
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC file_location = "/FileStore/tables/edges.csv"
// MAGIC file_type = "csv"
// MAGIC 
// MAGIC # CSV options
// MAGIC infer_schema = "false"
// MAGIC first_row_is_header = "false"
// MAGIC delimiter = ","
// MAGIC 
// MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
// MAGIC df1 = spark.read.format(file_type) \
// MAGIC   .option("inferSchema", infer_schema) \
// MAGIC   .option("header", first_row_is_header) \
// MAGIC   .option("sep", delimiter) \
// MAGIC   .load(file_location)
// MAGIC 
// MAGIC display(df1)

// COMMAND ----------

import org.apache.spark.rdd.RDD

// COMMAND ----------

import org.apache.spark.graphx._

// COMMAND ----------

val vertexRDD = sc.textFile("/FileStore/tables/vertex-1.csv")
val edgeRDD = sc.textFile("/FileStore/tables/edges.csv")
edgeRDD.collect()

// COMMAND ----------

vertexRDD.collect()

// COMMAND ----------

val vertices: RDD[(VertexId, (String,String))] = vertexRDD.map{ line => val fields = line.split(",")
                                                               (fields(0).toLong, ( fields(1), fields(2)))}
vertices.collect()

// COMMAND ----------

val edges: RDD[Edge[String]] = edgeRDD.map{ line => val fields = line.split(",")
                                           Edge(fields(0).toLong, fields(1).toLong,fields(2))}
edges.collect()

// COMMAND ----------

val default =("unknown","missing")
val graph = Graph(vertices, edges, default)

// COMMAND ----------

//Joining graphs

case class MoviesWatched(Movie: String, Genre: String)
val movies: RDD[(VertexId, MoviesWatched)] = sc.parallelize(List(
                                                                  (1, MoviesWatched("Toy Story 3", "Kids")),(2,MoviesWatched("Titanic","Love")),
                                                                    (3, MoviesWatched("The Hangover","Comedy"))))

// COMMAND ----------

val movieOuterJoinedGraph = graph.outerJoinVertices(movies)((_,name,movies) => (name,movies))

// COMMAND ----------

movieOuterJoinedGraph.vertices.map(t=>t).collect.foreach(println)

// COMMAND ----------

val movieOuterJoinedGraph = graph.outerJoinVertices(movies)((_,name,movies)=> (name,movies.getOrElse(MoviesWatched("NA","NA"))))

// COMMAND ----------

movieOuterJoinedGraph.vertices.map(t=>t).collect.foreach(println)

// COMMAND ----------

val tCount = graph.triangleCount().vertices

// COMMAND ----------

println(tCount.collect().mkString("\n"))

// COMMAND ----------

val iterations = 1000

// COMMAND ----------

val connected = graph.connectedComponents().vertices

// COMMAND ----------

val connectedS = graph.stronglyConnectedComponents(iterations).vertices

// COMMAND ----------

val connByPerson = vertices.join(connected).map{ case(id,((person,age), conn))=> (conn,id,person)}

// COMMAND ----------

val connByPersonS = vertices.join(connectedS).map{ case(id,((person,age), conn))=> (conn,id,person)}

// COMMAND ----------

connByPerson.collect().foreach{ case (conn,id,person)=> println(f"Weak $conn $id $person")}

// COMMAND ----------

connByPersonS.collect().foreach{case (conn,id,person)=> println(f"Strong $conn $id $person")}
