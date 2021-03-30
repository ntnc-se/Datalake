package com.viettel.datalake.config

import com.viettel.datalake.start.RunApplication

import java.io.FileInputStream
import java.util.Properties

object NotificationConfig {
  val properties: Properties = new Properties()
  val configFile: String = Option(RunApplication.configFile) match {
    case None => sys.error("Configure file not loaded")
    case Some(filePath) => filePath + "/notification.properties"
  }
  Option(new FileInputStream(configFile)) match {
    case None => sys.error("Configure file not found")
    case Some(resource) => {
      properties.load(resource)
    }
  }

  val PhonesNotify = properties.getProperty("phones.notify")
  var MissLog = properties.getProperty("miss.log")
  val api = properties.getProperty("api")
}
