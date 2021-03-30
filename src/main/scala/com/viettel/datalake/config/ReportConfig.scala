package com.viettel.datalake.config

import com.viettel.datalake.start.RunApplication

import java.io.FileInputStream
import java.util.Properties

object ReportConfig {
  val properties: Properties = new Properties()
  val configFile: String = Option(RunApplication.configFile) match {
    case None => sys.error("Configure file not loaded")
    case Some(filePath) => filePath + "/report.properties"
  }
  Option(new FileInputStream(configFile)) match {
    case None => sys.error("Configure file not found")
    case Some(resource) => {
      properties.load(resource)
    }
  }

  var XgamingUserInstallInDay = properties.getProperty("xgaming.user.install.in.day")
  var XgamingUserInstallInMonth = properties.getProperty("xgaming.user.install.in.month")
  var XgamingUserActiveAppInDay = properties.getProperty("xgaming.user.active.app.in.day")
  var XgamingUserActiveAppInMonth = properties.getProperty("xgaming.user.active.app.in.month")
  var XgamingUserActiveWapInDay = properties.getProperty("xgaming.user.active.wap.in.day")
  var XgamingUserActiveWapInMonth = properties.getProperty("xgaming.user.active.wap.in.month")
  var XgamingMatchTotalStreamerInDay = properties.getProperty("xgaming.match.total.streamer.in.day")
  var XgamingMatchTotalChallengeInDay = properties.getProperty("xgaming.match.total.challenge.in.day")
  var XgamingMatchTotalSuccessInDay = properties.getProperty("xgaming.match.total.success.in.day")
  var XgamingMatchTotalChallengerInDay = properties.getProperty("xgaming.match.total.challenger.in.day")
  var XgamingMatchHotStreamerInDay = properties.getProperty("xgaming.match.hot.streamer.in.day")
  var XgamingLiveTotalVideoInDay = properties.getProperty("xgaming.live.total.video.in.day")
  var XgamingLiveTotalWatcherInDay = properties.getProperty("xgaming.live.total.watcher.in.day")
  var XgamingLiveTotalViewInDay = properties.getProperty("xgaming.live.total.view.in.day")
  var XgamingLiveTotalMaxViewInDay = properties.getProperty("xgaming.live.total.max.view.in.day")
}
