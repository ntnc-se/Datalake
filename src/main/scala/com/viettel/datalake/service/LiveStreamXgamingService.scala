package com.viettel.datalake.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.viettel.datalake.config.{HadoopConfig, ReportConfig}
import com.viettel.datalake.dao.{LiveStreamXgamingDAO, ReportDAO}
import com.viettel.datalake.entity.LiveEntity
import com.viettel.datalake.utils.Helper

class LiveStreamXgamingService {
  val liveEntity = new LiveEntity
  val liveStreamXgamingDAO = new LiveStreamXgamingDAO
  val reportDAO = new ReportDAO
  
  def generateTotalVideoInDay(day: String): LiveEntity = {
    var ratioTotalVideoInDay:Float = 0
    var bTotalVideoInDay: Long = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusDays(1).toString.replace("-","").substring(0, 8)

    val totalStreamer = liveStreamXgamingDAO.totalVideoInDay(day)
    liveEntity._totalStreamerInDay = totalStreamer

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      bTotalVideoInDay = bReport.select(bReport("total_live_video_in_day")).where(bReport("createdday") === bDay).collect()(0).getLong(0)
      ratioTotalVideoInDay = Helper.ratio(totalStreamer, bTotalVideoInDay)
    }

    val report = ReportConfig.XgamingLiveTotalVideoInDay.replace(":total_video", totalStreamer.toString)
      .replace(":ratio_total_video", ratioTotalVideoInDay.toString)
    Helper.sendNotification(report, "0969664623")
    liveEntity._reportTotalSteamerInDay = report
    liveEntity
  }

  def generateTotalWatcherInDay(day: String): LiveEntity = {
    var ratioTotalWatcherInDay:Float = 0
    var bTotalWatcherInDay: Long = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusDays(1).toString.replace("-","").substring(0, 8)

    val totalStreamer = liveStreamXgamingDAO.totalWatcherInDay(day)
    liveEntity._totalWatcherInDay = totalStreamer

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      bTotalWatcherInDay = bReport.select(bReport("total_live_watcher_in_day")).where(bReport("createdday") === bDay).collect()(0).getLong(0)
      ratioTotalWatcherInDay = Helper.ratio(totalStreamer, bTotalWatcherInDay)
    }

    val report = ReportConfig.XgamingLiveTotalWatcherInDay.replace(":total_watcher", totalStreamer.toString)
      .replace(":ratio_total_watcher", ratioTotalWatcherInDay.toString)
    Helper.sendNotification(report, "0969664623")
    liveEntity._reportTotalWatcherInDay = report
    liveEntity
  }

  def generateTotalViewInDay(day: String): LiveEntity = {
    var ratioTotalViewInDay:Float = 0
    var bTotalViewInDay: Long = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusDays(1).toString.replace("-","").substring(0, 8)

    val totalStreamer = liveStreamXgamingDAO.totalWatch(day)
    liveEntity._totalViewInDay = totalStreamer

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      bTotalViewInDay = bReport.select(bReport("total_live_view_in_day")).where(bReport("createdday") === bDay).collect()(0).getLong(0)
      ratioTotalViewInDay = Helper.ratio(totalStreamer, bTotalViewInDay)
    }

    val report = ReportConfig.XgamingLiveTotalViewInDay.replace(":total_view", totalStreamer.toString)
      .replace(":ratio_total_view", ratioTotalViewInDay.toString)
    Helper.sendNotification(report, "0969664623")
    liveEntity._reportTotalViewInDay = report
    liveEntity
  }

  def generateMaxViewInDay(day: String): LiveEntity = {
    var ratioTotalMaxViewInDay:Float = 0
    var bTotalMaxViewInDay: Long = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusDays(1).toString.replace("-","").substring(0, 8)

    val totalStreamer = liveStreamXgamingDAO.maxView(day)
    liveEntity._maxViewInDay = totalStreamer

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      bTotalMaxViewInDay = bReport.select(bReport("total_live_max_view_in_day")).where(bReport("createdday") === bDay).collect()(0).getLong(0)
      ratioTotalMaxViewInDay = Helper.ratio(totalStreamer, bTotalMaxViewInDay)
    }

    val report = ReportConfig.XgamingLiveTotalMaxViewInDay.replace(":total_max_view", totalStreamer.toString)
      .replace(":ratio_total_max_view", ratioTotalMaxViewInDay.toString)

    liveEntity._reportMaxViewInDay = report
    liveEntity
  }
}
