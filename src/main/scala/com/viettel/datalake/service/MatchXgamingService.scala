package com.viettel.datalake.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.viettel.datalake.config.{HadoopConfig, ReportConfig}
import com.viettel.datalake.dao.{MatchXgamingDAO, ReportDAO}
import com.viettel.datalake.entity.MatchEntity
import com.viettel.datalake.utils.Helper

class MatchXgamingService {
  val matchEntity = new MatchEntity
  val matchXgamingDAO = new MatchXgamingDAO
  val reportDAO = new ReportDAO
  
  def generateTotalStreamer(day: String): MatchEntity = {
    var ratioTotalStreamer:Float = 0
    var bTotalStreamer: Long = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusDays(1).toString.replace("-","").substring(0, 8)

    val totalStreamer = matchXgamingDAO.totalStreamer(day)
    matchEntity._totalStreamerInDay = totalStreamer

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      bTotalStreamer = bReport.select(bReport("total_streamer_in_day")).where(bReport("createdday") === bDay).collect()(0).getLong(0)
      ratioTotalStreamer = Helper.ratio(totalStreamer, bTotalStreamer)
    }

    val report = ReportConfig.XgamingMatchTotalStreamerInDay.replace(":total_streamer", totalStreamer.toString)
      .replace(":ratio_total_streamer", ratioTotalStreamer.toString)
    Helper.sendNotification(report, "0969664623")
    matchEntity._reportTotalSteamerInDay = report
    matchEntity
  }

  def generateTotalChallenge(day: String): MatchEntity = {
    var ratioTotalMatch:Float = 0
    var bTotalMatch: Long = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusDays(1).toString.replace("-","").substring(0, 8)

    val totalMatch = matchXgamingDAO.totalMatch(day)
    matchEntity._totalChallengeInDay = totalMatch

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      bTotalMatch = bReport.select(bReport("total_challenge_in_day")).where(bReport("createdday") === bDay).collect()(0).getLong(0)
      ratioTotalMatch = Helper.ratio(totalMatch, bTotalMatch)
    }

    val report = ReportConfig.XgamingMatchTotalChallengeInDay.replace(":total_challenge", totalMatch.toString)
      .replace(":ratio_total_challenge", ratioTotalMatch.toString)
    Helper.sendNotification(report, "0969664623")
    matchEntity._reportTotalChallengeInDay = report
    matchEntity
  }

  def generateTotalChallenger(day: String): MatchEntity = {
    var ratioTotalChallenger:Float = 0
    var bTotalChallenger: Long = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusDays(1).toString.replace("-","").substring(0, 8)

    val totalChallenger = matchXgamingDAO.totalChanllenger(day)
    matchEntity._totalChallengerDay = totalChallenger

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      bTotalChallenger = bReport.select(bReport("total_challenge_in_day")).where(bReport("createdday") === bDay).collect()(0).getLong(0)
      ratioTotalChallenger = Helper.ratio(totalChallenger, bTotalChallenger)
    }

    val report = ReportConfig.XgamingMatchTotalChallengerInDay.replace(":total_challenger", totalChallenger.toString)
      .replace(":ratio_total_challenger", ratioTotalChallenger.toString)
    Helper.sendNotification(report, "0969664623")
    matchEntity._reportTotalChallengerInDay = report
    matchEntity
  }

  def generateHotStreamer(day: String): MatchEntity = {
    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    val hotStreamer = matchXgamingDAO.hotStreamer(day)
    matchEntity._hotSteamerInDay = hotStreamer

    val report = ReportConfig.XgamingMatchHotStreamerInDay.replace(":hot_streamer", hotStreamer)
    Helper.sendNotification(report, "0969664623")
    matchEntity._reportHotStreamerInDay = report
    matchEntity
  }

  def generateTotalSuccess(day: String): MatchEntity = {
    var ratioTotalSuccess:Float = 0
    var bTotalSuccess: Long = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusDays(1).toString.replace("-","").substring(0, 8)

    val totalSuccess = matchXgamingDAO.totalSuccess(day)
    matchEntity._totalSuccessInDay = totalSuccess

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      bTotalSuccess = bReport.select(bReport("total_success_in_day")).where(bReport("createdday") === bDay).collect()(0).getLong(0)
      ratioTotalSuccess = Helper.ratio(totalSuccess, bTotalSuccess)
    }

    val report = ReportConfig.XgamingMatchTotalSuccessInDay.replace(":total_success", totalSuccess.toString)
        .replace(":ratio_total_success", ratioTotalSuccess.toString)
    Helper.sendNotification(report, "0969664623")
    matchEntity._reportTotalSuccessInDay = report
    matchEntity
  }
}
