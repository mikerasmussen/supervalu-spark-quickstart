package com.supervalu.edh

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by hhmjr1 on 5/6/2016.
  */
object MetaFileHandler {

  // Use a Case Class to define the schema of a meta file
//  case class MetaFileSchema(SOURCE_NAME: String, SCHEMA_NAME: String, TABLE_NAME: String, COLUMN_NAME: String, DATA_TYPE: String,
//                            DATA_LENGTH: Int, DATA_SCALE: Int, FORMAT: String, PRIMARY_KEY: String, PI_DATA: String, COLUMN_ID: Int)

  case class MetaFileSchema(SOURCE_NAME: String, SCHEMA_NAME: String, TABLE_NAME: String, COLUMN_NAME: String, DATA_TYPE: String,
                            DATA_LENGTH: String, DATA_SCALE: String, FORMAT: String, PRIMARY_KEY: String, PI_DATA: String, COLUMN_ID: String)


  def main(args: Array[String]): Unit ={

    // make sure number of args passed in is correct
    // arg[0]:  data file
    // arg[1]:  ctl file
    // arg[2]:  meta file
//    if (args.length != 3) {
//      System.err.println("ERROR:  Wrong number of args passed in!")
//      System.exit(1)
//    }

    // setup a SparkContext
    val conf = new SparkConf().setAppName("MetaFileHandler").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this import is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._



    // Put meta info into a DataFrame (and apply a schema) so you can query the data type for each field
    val metaInfoDF = sc.textFile("file:///users/hhmjr1/EDW_PROMO_REWARD_DISCOUNTS.meta").map(_.split("|")).map(p => MetaFileSchema(p(0).trim, p(1).trim, p(2).trim, p(3).trim, p(4).trim,
      p(5).trim, p(6).trim, p(7).trim, p(8).trim, p(9).trim, p(10).trim)).toDF()


    
    // Register a temp table for the metadata file info
    metaInfoDF.registerTempTable("metadata")

    println()
    println("########################################")
    println("'metadata' schema is:")
    metaInfoDF.printSchema()
    println("########################################")
    println()


    // Test some SQL on the temp table
    val result = sqlContext.sql("SELECT COLUMN_NAME, DATA_TYPE from metadata")


    println()
    println("########################################")
    println("'result' is:   " + result)
    println("########################################")
    println()


    println()
    println("########################################")
    println("'result collect' is:")
    result.collect().foreach(println)
    println("########################################")
    println()




    // decide what checks/validations you want to do on the schema
      // compare the current schema to the one you have stored?
      // ensure the current data file conforms to the schema (meta file) provided?

      // what to do if the new schema (meta file) doesn't match?
          // add to 'superset' schema and store it?
          // throw an error or warning?  continue processing or fail it?



    // set the schema of the data file by using the meta info DF
      // selecting COLUMN_NAME and DATA_TYPE via SQL basically gives you a K:V to use.




  }

}
