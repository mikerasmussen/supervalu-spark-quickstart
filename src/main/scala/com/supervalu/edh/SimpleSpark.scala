package com.supervalu.edh

import com.supervalu.edh.common.AppLogging
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Created by hhmjr1 on 5/9/2016.
  */
object SimpleSpark {

  // One use of objects is when a Singleton is needed...like a logger.
  object SimpleSparkLogger extends AppLogging
    import SimpleSparkLogger.log


  def main(args: Array[String]){

    val conf = new SparkConf().setAppName(getClass.getName)
    val sc = new SparkContext(conf)

    val autoFile = sc.textFile("hdfs://nameservice1/user/hhmjr1/autos2.csv")

//    println()
//    println("########################################")
//    println("'autoFile' is:")
//    autoFile.collect().foreach(println)
//    println("########################################")
//    println()

    log.info("")
    log.info("########################################")
    log.info("'autoFile' is:")
    //log.info(autoFile.collect().foreach(println).toString)
    autoFile.collect().foreach(element => log.info(element))
    log.info("########################################")
    log.info("")

    //val counts = autoFile.flatMap(line => line.split(",")).map(word => (word, 1)).reduceByKey(_ + _)
    //val lines = autoFile.map(line => line.split("\n"))
    val chevys = autoFile.filter(f => f.contains("chevrolet"))
    val items = autoFile.flatMap(line => line.split(","))
    //lines.saveAsTextFile("file:///users/hhmjr1/counts/...")

//    println()
//    println("########################################")
//    println("'chevys' is:")
//    chevys.collect().foreach(println)
//    println("########################################")
//    println()

    log.info("")
    log.info("########################################")
    log.info("'chevys' is:")
    //log.info(chevys.collect().foreach(println).toString)
    chevys.collect().foreach(element => log.info(element))
    log.info("########################################")
    log.info("")

//    println()
//    println("########################################")
//    println("'items' is:")
//    items.collect().foreach(println)
//    println("########################################")
//    println()

    log.info("")
    log.info("########################################")
    log.info("'items' is:")
    items.collect().foreach(element => log.info(element))
    log.info("########################################")
    log.info("")

    sc.stop()
  }
}
