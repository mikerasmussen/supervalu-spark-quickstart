package com.supervalu.edh

/**
  * Created by hhmjr1 on 11/12/2015.
  */

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.io.Source

import math.{sqrt, pow}


object SparkTest1 {


  def saveAsParquet(): Unit ={
    // RDDs cannot be saved to Parquet directly.  ONLY DATAFRAMES CAN BE SAVED TO PARQUET DIRECTLY B/C A SCHEMA IS REQUIRED

    // Pass in RDD as arg
    // Convert RDD to DataFrame
    // Save DataFrame as Parquet file


//    val inputPath = "../data/json"
//    val outputPath = "../data/parquet"
//    val data = sqlContext.read.json(inputPath)
//    date.write.parquet(outputPath)


  }




  def main(args: Array[String]){

    // make sure number of args passed in is correct
      // arg[0]:  host (e.g. local)
      // arg[1]:  file to process
    if (args.length < 2) {
      System.err.println("Usage: SparkGrep <host> <input_file>")
      System.exit(1)
    }

    // setup a SparkContext
    val conf = new SparkConf().setAppName("SparkTest1").setMaster(args(0))
    val sc = new SparkContext(conf)





    // Function to output a JSON file from a DataFrame.
    def dataFrameToJson(df: DataFrame): RDD[String] = {
      df.toJSON
    }



    // Function to calculate the distance between 2 points in 2D space.
    def calculateDistance(x1: Double, y1: Double, x2: Double, y2: Double): Double ={
      // The distance formula
      sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))
    }

    println()
    println("########################################")
    println("'calculateDistance' is:")
    println(calculateDistance(1.0, 1.0, 7.0, 9.0))
    println("########################################")
    println()


    // Get station # and lat and long into list
    // Get store # and lat and long into list

    // iterate through the store list
    // for each store, calc the distance to each station
    // Store output in a list
    // Looks like this:  List store1 = (

    //val x = sc.parallelize(List(('a',10,1),('b',2,2),('c',3,3)))

    //val storeLatLong = sc.textFile("file:///users/hhmjr1/storeLatLong.txt")         // RDD of Strings
    //val stationLatLong = sc.textFile("file:///users/hhmjr1/stationLatLong.txt")     // RDD of Strings

    //stationLatLong.map(calculateDistance(storeLatLong._2, storeLatLong._3, stationLatLong._2,stationLatLong._3)
    // storeLatLong.map(stationLatLong.map(calculateDistance(storeLatLong._2, storeLatLong._3, stationLatLong._2,stationLatLong._3)))


    val storeLatLongArray = Source.fromFile("/users/hhmjr1/storeLatLong.txt").getLines.toArray
    val stationLatLongArray = Source.fromFile("/users/hhmjr1/stationLatLong.txt").getLines.toArray
    val storeLatLongList = Source.fromFile("/users/hhmjr1/storeLatLong.txt").getLines.toList
    val stationLatLongList = Source.fromFile("/users/hhmjr1/stationLatLong.txt").getLines.toList


    println()
    println("########################################")
    println("'storeLatLongArray' is:")
    println(storeLatLongArray)
    println("########################################")
    println()

    println()
    println("########################################")
    println("'stationLatLongArray' is:")
    println(stationLatLongArray)
    println("########################################")
    println()

    println()
    println("########################################")
    println("'storeLatLongList' is:")
    println(storeLatLongList)
    println("########################################")
    println()

    println()
    println("########################################")
    println("'stationLatLongList' is:")
    println(stationLatLongList)
    println("########################################")
    println()


    println()
    println("########################################")
    println("'storeLatLongList' is:")
    //println(storeLatLongList._1)
    println("########################################")
    println()



    //storeLatLong.foreach(calculateDistance(storeLatLong[1], storeLatLong[2], stationLatLong[1], stationLatLong[2]))















    //@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // Read in NOAA JSON files from RAW (e.g. hdfs://data/raw/marketing/analytics_weather/noaa/*.json)
    val noaaJson = sc.textFile(args(1))    // noaaJson is RDD of Strings

    // noaaJson.collect().foreach(println) looks like this:
    //{"metadata":{"resultset":{"offset":1,"count":45,"limit":1000}},"results":[{"date":"2015-03-10T00:00:00","datatype":"PRCP","station":"GHCND:US1MOOR0023","attributes":",,N,","value":234},{"date":"2015-03-26T00:00:00","datatype":"PRCP","station":"GHCND:US1MOOR0023","attributes":",,N,","value":488},{"date":"2015-03-31T00:00:00","datatype":"PRCP","station":"GHCND:US1MOOR0023","attributes":",,N,","value":0}]}

    println()
    println("########################################")
    println("'noaaJson' is:")
    noaaJson.collect().foreach(println)
    println("########################################")
    println()

    // Use flatMap to break json into lines based on "},"
    // Get rid of the "metadata" line -- {"metadata":{"resultset":{"offset":1,"count":45,"limit":1000}
    val resultsSplit = noaaJson.flatMap(lines => lines.split("},").filterNot(f=> f.contains("metadata")))
    // resultsSplit is RDD of Array of Strings

    // resultsSplit.collect().foreach(println) looks like this:
    //"results":[{"date":"2015-03-10T00:00:00","datatype":"PRCP","station":"GHCND:US1MOOR0023","attributes":",,N,","value":234
    //{"date":"2015-03-26T00:00:00","datatype":"PRCP","station":"GHCND:US1MOOR0023","attributes":",,N,","value":488
    //{"date":"2015-03-31T00:00:00","datatype":"PRCP","station":"GHCND:US1MOOR0023","attributes":",,N,","value":0}]}

    println()
    println("########################################")
    println("'resultsSplit' is:")
    resultsSplit.collect().foreach(println)
    println("########################################")
    println()


//    val lineWithResultsTag = resultsSplit.filter(f=> f.contains("results"))
//
//    println()
//    println("########################################")
//    println("'lineWithResultsTag' is:")
//    lineWithResultsTag.collect().foreach(println)
//    println("########################################")
//    println()


    // Strip off the 'results' array that holds the json objects
      // Use map to iterate thru all lines of resultsSplit
      // Get rid of "results":[ found in 1st line
      // Get rid of }]} found in last line
      // Add } to end of each line to close the json object
    //val json4DataFrame = resultsSplit.map(line => line.replace("\"results\":[", "")).map(line => line.replace("}]}", "")).map(line => line.concat("}"))
    val arrayName = "results"
    val arrayToModify = "\"" + arrayName + "\":["
    val json4DataFrame = resultsSplit.map(line => line.replace(arrayToModify, "")).map(line => line.replace("}]}", "")).map(line => line.concat("}"))

    // json4DataFrame.collect().foreach(println) looks like this:
    //{"date":"2015-03-10T00:00:00","datatype":"PRCP","station":"GHCND:US1MOOR0023","attributes":",,N,","value":234}
    //{"date":"2015-03-26T00:00:00","datatype":"PRCP","station":"GHCND:US1MOOR0023","attributes":",,N,","value":488}
    //{"date":"2015-03-31T00:00:00","datatype":"PRCP","station":"GHCND:US1MOOR0023","attributes":",,N,","value":0}

    println()
    println("########################################")
    println("'json4DataFrame' is:")
    json4DataFrame.collect().foreach(println)
    println("########################################")
    println()



    // Load JSON data into DataFrame
    val noaaDF = sqlContext.jsonRDD(json4DataFrame)
    //val noaaDF = hiveContext.jsonRDD(json4DataFrame)



    println()
    println("########################################")
    println("'noaaDF' schema is:")
    noaaDF.printSchema()
    println("########################################")
    println()


    val df = sqlContext.jsonRDD(noaaJson)

    println()
    println("########################################")
    println("'df' schema is:")
    df.printSchema()
    println("########################################")
    println()


//    'df' schema is:
//      root
//    |-- metadata: struct (nullable = true)
//    |    |-- resultset: struct (nullable = true)
//    |    |    |-- count: long (nullable = true)
//    |    |    |-- limit: long (nullable = true)
//    |    |    |-- offset: long (nullable = true)
//    |-- results: array (nullable = true)
//    |    |-- element: struct (containsNull = true)
//    |    |    |-- attributes: string (nullable = true)
//    |    |    |-- datatype: string (nullable = true)
//    |    |    |-- date: string (nullable = true)
//    |    |    |-- station: string (nullable = true)
//    |    |    |-- value: long (nullable = true)




    println()
    println("########################################")
    println("'df' is:")
    df.collect().foreach(println)
    println("########################################")
    println()




    df.toJSON.saveAsTextFile("/user/hhmjr1/jsonOut/") //sends 'part-00001' file to this path...path must NOT exist

    println()
    println("########################################")
    println("'df back to JSON' is:")
    //df.toJSON.take(2).foreach(println)  //There's only 1 obj in the JSON so take(2) returns same as collect()
    df.toJSON.collect().foreach(println)
    println("########################################")
    println()



    // Do Paul's "Transformation" work on the data:

//    // Register this DataFrame as a table.
//    noaaDF.registerTempTable("noaa_weather")
//    val prcp = sqlContext.sql("SELECT MAX(value), MIN(value), AVG(value) from noaa_weather where datatype='PRCP'")
//    val snow = sqlContext.sql("SELECT MAX(value), MIN(value), AVG(value) from noaa_weather where datatype='SNOW'")
//    val tmin = sqlContext.sql("SELECT MAX(value), MIN(value), AVG(value) from noaa_weather where datatype='TMIN'")
//    val tmax = sqlContext.sql("SELECT MAX(value), MIN(value), AVG(value) from noaa_weather where datatype='TMAX'")



    // Register this DataFrame as a table.
    df.registerTempTable("weather")

//    //val testPRCP = sqlContext.sql("SELECT results.value from weather")
//    val testPRCP = sqlContext.sql("SELECT results.value from weather where results.value>0.0L")
//
//    println()
//    println("########################################")
//    println("'testPRCP' is:")
//    println(testPRCP.collect().foreach(println))
//    println("########################################")
//    println()


    val result = sqlContext.sql("Select * from weather")
    println()
    println("########################################")
    println("'result' is:")
    println(result.collect().foreach(println))
    println("########################################")
    println()

    sqlContext.sql("insert into weather values('value_1','value_2','value_3','value_4','value_5'")



    df.col("results.value").cast("int")
    //val dateVal = sqlContext.sql("SELECT results.station from weather where CAST(results.value AS int) intVal>0")
    //val dateVal = sqlContext.sql("SELECT results.station from weather where CAST(results.value as int)>0")
    val dateVal = sqlContext.sql("SELECT results.station from weather where results.value > 0")

    println()
    println("########################################")
    println("'dateVal' is:")
    println(dateVal.collect().foreach(println))
    println("########################################")
    println()





//    val prcp = sqlContext.sql("SELECT MAX(results.value), MIN(results.value), AVG(results.value) from weather where results.datatype='PRCP'")
//    val snow = sqlContext.sql("SELECT MAX(results.value), MIN(results.value), AVG(results.value) from weather where results.datatype='SNOW'")
//    val tmin = sqlContext.sql("SELECT MAX(results.value), MIN(results.value), AVG(results.value) from weather where results.datatype='TMIN'")
//    val tmax = sqlContext.sql("SELECT MAX(results.value), MIN(results.value), AVG(results.value) from weather where results.datatype='TMAX'")

    val prcp = sqlContext.sql("SELECT MAX(results.value) from weather")
    val snow = sqlContext.sql("SELECT MIN(results.value) from weather")
    val tmin = sqlContext.sql("SELECT AVG(results.value) from weather")


//    val prcp = sqlContext.sql("SELECT MAX(results.value) from weather where results.datatype='PRCP'")
//    val snow = sqlContext.sql("SELECT MAX(results.value) from weather where results.datatype='SNOW'")
//    val tmin = sqlContext.sql("SELECT MAX(results.value) from weather where results.datatype='TMIN'")
//    val tmax = sqlContext.sql("SELECT MAX(results.value) from weather where results.datatype='TMAX'")


    println()
    println("########################################")
    println("'prcp' is:")
    println(prcp.collect().foreach(println))
    println()
    println("'snow' is:")
    println(snow.collect().foreach(println))
    println()
    println("'tmin' is:")
    println(tmin.collect().foreach(println))
    println()
    println("'tmax' is:")
  // println(tmax.collect().foreach(println))
    println("########################################")
    println()








//    val dateFormat = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss")
//    val now = dateFormat.format(Calendar.getInstance().getTime())
//    noaaDF.rdd.saveAsTextFile("hdfs:///user/hhmjr1/noaaText_")
//    noaaDF.saveAsParquetFile("hdfs:///user/hhmjr1/noaaParquet_")

    // This doesn't work for some reason...Must investigate stacktrace.
    //noaaDF.saveAsTable("noaa_weather")


//    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
//    val items = resultsSplit.map(s=>s.toString)
//    println()
//    println("########################################")
//    println("'items' is:")
//    items.collect().foreach(println)
//    println("########################################")
//    println()

//    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
//    val items = resultsSplit.filter(f=> f.contains("metadata"))
//    println()
//    println("########################################")
//    println("'items' is:")
//    items.collect().foreach(println)
//    println("########################################")
//    println()
//
//
//    val firstItem = resultsSplit.map(s => s.contains("metadata"))
//    println()
//    println("########################################")
//    println("'firstItem' is:")
//    firstItem.collect().foreach(println)
//    println("########################################")
//    println()



//@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@








//    // make 2 internal datasets (Parallelized Collection)
//    val zeroThruNine = sc.parallelize(0 to 9, 1)      // an RDD of ints
//    //zeroThruNine.collect()
//    println()
//    println("########################################")
//    println("'zeroThruNine' is:")
//    zeroThruNine.collect().foreach(println)
//    println("########################################")
//    println()
//
//    val data = Array(10, 12, 14, 16, 18, 20)
//    val evensTenThruTwenty = sc.parallelize(data)     // an RDD of ints
//    println()
//    println("########################################")
//    println("'evensTenThruTwenty' is:")
//    evensTenThruTwenty.collect().foreach(println)
//    println("########################################")
//    println()
//
//    // union 2 datasets
//    val zeroThruTwenty = zeroThruNine.union(evensTenThruTwenty)    // returns an RDD of ints
//
//    println()
//    println("########################################")
//    println("'zeroThruTwenty' is:")
//    zeroThruTwenty.collect().foreach(println)
//    println("########################################")
//    println()
//
//    // get an external dataset (HDFS or Linux file)       [e.g.  file:///users/hhmjr1/oddNumbers.txt]
//    val oddsThruTwentyNine = sc.textFile(args(1))    // an RDD of strings
//    println()
//    println("########################################")
//    println("'oddsThruTwentyNine' is:")
//    oddsThruTwentyNine.collect().foreach(println)
//    println("########################################")
//    println()
//
//
////    // test out how to replace a value with another value in a file
////    val replace29with31 = oddsThruTwentyNine.map(x => x.map(_.replace("29","31")))
////    println()
////    println("########################################")
////    println("'replace29with31' is:")
////    replace29with31.collect().foreach(println)
////    println("########################################")
////    println()
//
//
//
//    // The below filter is pretty dumb b/c your RDD is of Strings not Int. I don't think you can union/join on String RRD
//    // like you can with Int (although I see a union example on Strings in ch3 of the Spark book)
//
//    // filter dataset
//    //val oddsElevenThruTwentyNine = oddsThruTwentyNine.filter(line => line.contains("11 13 15 17 19 21 23 25 27 29"))
//
////    val odds = oddsThruTwentyNine.flatMap(x => x.split(" "))
////    val oddInts = odds.flatMap(x => x.toInt)
////
////    println()
////    println("########################################")
////    println("'oddInts' is:")
////    oddInts.collect().foreach(println)
////    println("########################################")
////    println()
//
////    println()
////    println("########################################")
////    println("'oddsElevenThruTwentyNine' is:")
////    oddsElevenThruTwentyNine.collect().foreach(println)
////    println("########################################")
////    println()
//
//
//    // This filter works fine
////    // get an external dataset (HDFS or Linux file)       [e.g.  file:///users/hhmjr1/autos.csv]
////    val autos = sc.textFile(args(1))    // an RDD of strings
////    val chevys = autos.filter(x => x.contains("chevrolet"))
////    println()
////    println("########################################")
////    println("'chevys' is:")
////    chevys.collect().foreach(println)
////    println("########################################")
//
//
//    // query the joined result set
//
//    // write RDD out to a file (HDFS or Linux)  (compress file, Parquet or Avro too)
//
//    // stop the SparkContext


    sc.stop()
  }
}
