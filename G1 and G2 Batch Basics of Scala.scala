// Databricks notebook source
var num=List(1,2,3,4)

// COMMAND ----------

num.head

// COMMAND ----------

num.tail

// COMMAND ----------

num.sum

// COMMAND ----------

num.take(2)

// COMMAND ----------

var samir = List(1,1,1,1,2,2,2,2)

// COMMAND ----------

samir.distinct

// COMMAND ----------

num(0)

// COMMAND ----------

num(1)

// COMMAND ----------

num(1)=10

// COMMAND ----------

num.size

// COMMAND ----------

num.reverse

// COMMAND ----------

num.min

// COMMAND ----------

num.max

// COMMAND ----------

num.isEmpty

// COMMAND ----------

var number=Array(1,2,3,4,5,6,7,8)

// COMMAND ----------

val lang = Array("Scala","python","spark")

// COMMAND ----------

lang.head

// COMMAND ----------

lang.tail

// COMMAND ----------

number(1)=10

// COMMAND ----------

number

// COMMAND ----------

import scala.collection.mutable.ArrayBuffer

// COMMAND ----------

var cars = new ArrayBuffer[String]()

// COMMAND ----------

cars +="BMW"

// COMMAND ----------

cars +="Jaguar"
cars+="Jaguar"

// COMMAND ----------

cars.length

// COMMAND ----------

cars.trimEnd(1)

// COMMAND ----------

cars

// COMMAND ----------

cars.insert(2,"Bentley")

// COMMAND ----------

cars

// COMMAND ----------

val num = List(1,2,3,4)

// COMMAND ----------

num.map(y => y*y)

// COMMAND ----------

num

// COMMAND ----------

val a = num.map(x => x+1)

// COMMAND ----------

val b = a.map(x => x*x)

// COMMAND ----------

val c = b.map(x => x-1)

// COMMAND ----------

val d = c.map(x => -x)

// COMMAND ----------

num.map(x=>x+1).map(x=>x*x).map(x=>x-1).map(x=> -x)

// COMMAND ----------

val fruits = List("Orange","banana","Apple","Pomogranate")

// COMMAND ----------

fruits.map(x => (x,x.length))

// COMMAND ----------

fruits.filter( x => x.length > 5)

// COMMAND ----------

val ratings = List(2.4,5.6,7.4,6.5,8.9,9.0,7.8)

// COMMAND ----------

val marks = ratings.map(x => x*10)

// COMMAND ----------

val grademarks = marks.filter(x => x >= 60 && x <= 74)

// COMMAND ----------

val graderatings= grademarks.map(x => x/10)

// COMMAND ----------

def add(a:Double=100,b:Double=200):Double = {
  var sum:Double =0
  sum = a + b
  return sum
}
println("sum :" +add())

// COMMAND ----------

var x =4
 val y = if(x<3)
  println("Value of x is less than 3")
else
  println("Value of x  is greater than or equal to 3")

// COMMAND ----------

val marks =61
if (marks >=70)
{
  println("Distinction")
} else if (marks >=60 && marks <70)
  println("First Class")
 else if (marks >=50 && marks <60)
  println("Second Class")
 else if (marks >=40 && marks <50)
  println("Pass Class")
 else println("Fail")

// COMMAND ----------

def square(x:Double):Double = {
  return x*x
}
def sumsquares(x:Double, y:Double):Double = {
  return square(x) + square(y)
}
println("Sum of squares :" +sumsquares(4,5))

// COMMAND ----------

def time():Long = {
 println("Inside Time Function ")
  return System.nanoTime()
}

def exec(t:Long):Long = {
  println("Inside Exec Function")
  println("Time :" +t)
  println("Exiting from EXEC function")
  return t
}
println("Main Function :" +exec(time()))

// COMMAND ----------

var i = 10
while( i>0)
{
  println("Hello :" +i)
  i=i-1
}

// COMMAND ----------

var a = 2
do{
  println(a)
  a+=2
}while(a<=10)

// COMMAND ----------

object classEg{
  def main(arg:Array[String]){
    var ob = new NewClass("Hello World")
    ob.sayHi()
  }
}

class NewClass(mssg:String){
  def sayHi() = println(mssg)
}

// COMMAND ----------

for (i <- 1 to 10)
 println(i)

// COMMAND ----------


