package com.viettel.datalake.config

import com.viettel.datalake.start.RunApplication

import java.io.FileInputStream
import java.util.Properties

object GroupIdConfig {
  val properties: Properties = new Properties()
  val configFile: String = Option(RunApplication.configFile) match {
    case None => sys.error("Configure file not loaded")
    case Some(filePath) => filePath + "/group_id.properties"
  }
  Option(new FileInputStream(configFile)) match {
    case None => sys.error("Configure file not found")
    case Some(resource) => {
      properties.load(resource)
    }
  }

  val MytelGroupId = properties.getProperty("mytel.group.id")
  val UnitelGroupId = properties.getProperty("unitel.group.id")
  val MetfoneGroupId = properties.getProperty("metfone.group.id")
  val XgamingGroupId = properties.getProperty("xgaming.group.id")
  val TestGroupId = properties.getProperty("test.group.id")
  val ErrorGroupId = properties.getProperty("error.group.id")
}
