package com.supervalu.edh

import com.supervalu.edh.common.AppLogging
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by hhmjr1 on 5/2/2016.
  */
object SparkLogTestDriver {

  // One use of objects is when a Singleton is needed...like a logger.
  object DriverLogger extends AppLogging
    import DriverLogger.log


  def main(args: Array[String]) {

    // make sure number of args passed in is correct
    // arg[0]:  ratings file
    // arg[1]:  movies file
    if (args.length != 2) {
      log.error("ERROR:  Wrong number of args passed into program! Expecting 2 args.")
      System.exit(1)
    }

    // setup a SparkContext
    val conf = new SparkConf().setAppName("SparkLogTest")
    val sc = new SparkContext(conf)

    val ratingsRdd = sc.textFile(args(0))   // RDD of Strings
    val moviesRdd = sc.textFile(args(1))    // RDD of Strings

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val ratingsDF = sqlContext.jsonRDD(ratingsRdd)
      log.info("Successfully created 'ratingsDF' DataFrame.")
    val moviesDF = sqlContext.jsonRDD(moviesRdd)
      log.info("Successfully created 'moviesDF' DataFrame.")

    log.info("")
    log.info("########################################")
    log.info("'ratingsDF' schema is:")
    log.info(ratingsDF.printSchema().toString)
    log.info("########################################")
    log.info("")

    log.info("")
    log.info("########################################")
    log.info("'moviesDF' schema is:")
    log.info(moviesDF.printSchema().toString)
    log.info("########################################")
    log.info("")


    // Test logging on function outside of this Driver class
    val joinedDF = SparkLogTest.innerJoin(ratingsDF, moviesDF, "movieId")

    // Test logging on function outside of this Driver class & within a lambda function
    SparkLogTest.forEachLoggingTest(joinedDF)

  }
}
