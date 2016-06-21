package com.supervalu.edh

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hhmjr1 on 6/21/2016.
  */
object GitPractice {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(getClass.getName)
    val sc = new SparkContext(conf)

    val zeroThruNine = sc.parallelize(0 to 9, 1)      // an RDD of ints

    println()
    println("########################################")
    println("'zeroThruNine' is:")
    zeroThruNine.collect().foreach(println)
    println("########################################")
    println()


    val data = Array(10, 12, 14, 16, 18, 20)
    val evensTenThruTwenty = sc.parallelize(data)     // an RDD of ints
    println()
    println("########################################")
    println("'evensTenThruTwenty' is:")
    evensTenThruTwenty.collect().foreach(println)
    println("########################################")
    println()

  }

}
