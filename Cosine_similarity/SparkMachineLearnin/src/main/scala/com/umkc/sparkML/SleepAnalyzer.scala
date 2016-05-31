package com.umkc.sparkML

import java.io.{File, PrintWriter}
import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField}

/**
  * Created by Hiren Shah on 5/3/2016.
  */
object SleepAnalyzer   {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName("MovieLensALS")
      .set("spark.executor.memory", "2g").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //mysql connection
    val databaseUsername = "heartfit_root"
    val databasePassword = "heart123"
    val databaseConnectionUrl = "jdbc:mysql://srv70.hosting24.com:3306/heartfit_123?user=" + databaseUsername + "&password=" + databasePassword

    val rdd = new JdbcRDD( sc, () =>
      DriverManager.getConnection(databaseConnectionUrl,databaseUsername,databasePassword) ,
      "select format(rate_x,0) rate_x,format(rate_y,0) rate_y,format(rate_z,0) rate_z,read_date,read_hour,read_min,ID from accelerometer limit ?, ?",
      1, 50, 2, r => r.getString("rate_x") + " " + r.getString("rate_y") + " " + r.getString("rate_z") + " " + r.getString("read_date") + " " + r.getString("read_hour") + " " + r.getString("read_min") + " " + r.getString("ID"))

    val dataset = rdd
      .map(line => {
        val fields = line.split(" ")
        (fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3), fields(4).toInt, fields(5).toInt, fields(6))
      })

    val pairs = dataset.cartesian(dataset)

    val filter = pairs.filter(f => (f._1._1 != f._2._1 || f._1._2 != f._2._2 || f._1._3 != f._2._3) && f._1._4 == f._2._4 && f._1._5 == f._2._5 && f._1._6 == f._2._6 && f._1._7 != f._2._7)

   // filter.collect().foreach(println)

    val result = filter.map(r => {
      val key = (r._1._1,r._1._2,r._1._3,r._1._4,r._1._5,r._1._6,r._1._7)
      val value = 1;
      (key, value)
    }).reduceByKey(_ + _)

    //result.collect().foreach(println)


    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._


   val df =  result.map {
     case (k, v) => acc(k._1, k._2, k._3, k._4, k._5, k._6,v, k._7.toInt) }
     .toDF()

    val accl = Window.partitionBy('out_date,'out_hour,'out_min).orderBy('out_count.asc)

    val ranked = df.withColumn("rank", row_number().over(accl))
    ranked.registerTempTable("resultitems")

    val finalresults = sqlContext.sql("SELECT out_x,out_y,out_z,out_date,out_hour,out_min,out_count FROM resultitems where rank = 1")

    finalresults.collect().foreach(println)

   //finalresults.insertIntoJDBC(databaseConnectionUrl,"output_accelerometer",false)

  }

  def GetHour(hour : Int) = {

    val result = if(hour == 1)
                    13
    else if(hour == 2)
       14
    else if(hour == 3)
      15
    else if(hour == 4)
      16
    else if(hour == 5)
      17
    else if(hour == 6)
      18
    else if(hour == 7)
      19
    else if(hour == 8)
      20
    else if(hour == 9)
      21
    else if(hour == 10)
      12
    else if(hour == 11)
      23

    result
  }


  case class acc(out_x: Int,out_y: Int, out_z: Int, out_date: String, out_hour: Int, out_min: Int, out_count: Int, ID : Int)

}

