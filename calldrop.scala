package com.svk

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types._


object calldrop {
  
  case class Call(id:Int, sec:Int, lan:String, ph:String)
  
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
                .builder()
                .appName("call drop")
                .master("local[*]")
                .getOrCreate()
    
     import spark.implicits._
     
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
     
     val cdr = spark.sparkContext.textFile("file:///C:/SparkScala/CDR.csv")
                                 .map(x => x.split(","))
                                 .map(x => Call(x(0).trim.toInt,x(1).trim.toInt,x(2),x(3)))
                                 .toDF()
     
    //cdr.printSchema()
     
    cdr.createOrReplaceTempView("calldrop")

    val a = spark.sql("select id,count(sec) as cnt from calldrop group by id order by cnt desc limit 10 ")
    
    val b = spark.sql("select id, ph, lan,sec from calldrop where sec in (1) group by id,ph,lan,sec order by sec")
    
    
    b.createOrReplaceTempView("finaldrop")
    
    val table = spark.sql("select id,sum(sec) as cnt from finaldrop group by id order by cnt desc limit 10").show() 
    
    
  }
}
  
  
