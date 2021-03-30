package com.viettel.datalake.config

import com.viettel.datalake.start.RunApplication

import java.io.FileInputStream
import java.util.Properties

object MochaConfig {
  val properties: Properties = new Properties()
  val configFile: String = Option(RunApplication.configFile) match {
    case None => sys.error("Configure file not loaded")
    case Some(filePath) => filePath + "/mocha.properties"
  }
  Option(new FileInputStream(configFile)) match {
    case None => sys.error("Configure file not found")
    case Some(resource) => {
      properties.load(resource)
    }
  }

  val MochaApi = properties.getProperty("mocha.api")
  val UserName = properties.getProperty("username")
  val Password = properties.getProperty("password")
  val FromMsisdn = properties.getProperty("from.msisdn")
}
