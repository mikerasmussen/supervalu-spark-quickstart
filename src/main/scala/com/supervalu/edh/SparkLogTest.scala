package com.supervalu.edh

import com.supervalu.edh.common.AppLogging
import org.apache.spark.sql.DataFrame

/**
  * Created by hhmjr1 on 5/2/2016.
  */
object SparkLogTest{

  object ExecutorLogger extends AppLogging
  import ExecutorLogger.log
  /*
  ###################################################################################################################
    YOU MIGHT HAVE TO MOVE THE ABOVE IMPORT INSIDE THE 'INNERJOIN' FUNCTION FOR IT WORK PROPERLY FOR DIST ACTIONS.
  ###################################################################################################################
   */


  // Unnecessary function to test logging in a class outside the Driver.
  def innerJoin(df1:  DataFrame, df2:  DataFrame, joinCol: String): DataFrame = {

    log.info("")
    log.info("Inside 'innnerJoin' function call...")
    log.info("")

    //df1.join(df2, df1("movieId") === df2("movieId"), "inner")
    df1.join(df2, df1(joinCol) === df2(joinCol), "inner")
  }

  def forEachLoggingTest(df: DataFrame): Unit = {
    //df.take(10).foreach(element => log.info(element))

    log.info("")
    log.info("Inside 'forEachLoggingTest' function call...")
    log.info("")

    // If DataFrame passed in is large, multiple executors will run.  Where does the logging go?
    df.foreach(element => log.info(element.toString()))
  }

}
