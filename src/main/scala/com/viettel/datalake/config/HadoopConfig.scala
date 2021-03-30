package com.viettel.datalake.config

import com.viettel.datalake.start.RunApplication

import java.io.FileInputStream
import java.util.Properties

object HadoopConfig {
  val properties: Properties = new Properties()
  val configFile: String = Option(RunApplication.configFile) match {
    case None => sys.error("Configure file not loaded")
    case Some(filePath) => filePath + "/hadoop.properties"
  }
  Option(new FileInputStream(configFile)) match {
    case None => sys.error("Configure file not found")
    case Some(resource) => {
      properties.load(resource)
    }
  }

  val HdfsPath = properties.getProperty("hdfs.path")
  val HdfsUrl = properties.getProperty("hdfs.url")
  val HdfsUser = properties.getProperty("hdfs.user")
  val MytelReportPath = properties.getProperty("mytel.report.path")
  val UnitelReportPath = properties.getProperty("unitel.report.path")
  val MetfoneReportPath = properties.getProperty("metfone.report.path")

  //Xgaming
  val XgamingActivePath = properties.getProperty("xgaming.active.path")
  val XgamingDevicePath = properties.getProperty("xgaming.device.path")
  val XgamingMatchPath = properties.getProperty("xgaming.match.path")
  val XgamingSubUserPath = properties.getProperty("xgaming.subuser.path")
  val XgamingVideoPath = properties.getProperty("xgaming.video.path")
  val XgamingReportPath = properties.getProperty("xgaming.report.path")

  //Mocha
  val MochaUserMusicRoomPath = properties.getProperty("mocha.music.room.path")
}
