package com.supervalu.edh.common

import org.slf4j.LoggerFactory

/**
  * Created by hhmjr1 on 5/9/2016.
  */

  // Create an 'interface' for logging.
  trait AppLogging extends Serializable {
    //@transient lazy val log = Logger.getLogger(getClass.getName)
    @transient lazy val log = LoggerFactory.getLogger(getClass.getName)   // change from log4j to slf4
}
