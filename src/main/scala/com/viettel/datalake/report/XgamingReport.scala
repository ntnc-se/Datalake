package com.viettel.datalake.report

import com.viettel.datalake.service.{ActiveXgamingService, DeviceXgamingService, LiveStreamXgamingService, MatchXgamingService}
import com.viettel.datalake.start.RunApplication
import com.viettel.datalake.utils.Helper
import org.apache.spark.sql.Row

object XgamingReport {
  def report(day: String): String = {
    val spark = RunApplication.spark
    val deviceXgamingService = new DeviceXgamingService
    val activeXgamingService = new ActiveXgamingService
    val liveStreamXgamingService = new LiveStreamXgamingService
    val matchXgamingService = new MatchXgamingService

    val deviceEntityInDay = deviceXgamingService.generateNewInstallInDay(day)
    val deviceEntityInMonth = deviceXgamingService.generateNewInstallInMonth(day)
    val activeAppEntityInDay = activeXgamingService.generateActiveAppInDay(day)
    val activeAppEntityInMonth = activeXgamingService.generateActiveAppInMonth(day)
    val activeWapEntityInDay = activeXgamingService.generateActiveWapInDay(day)
    val activeWapEntityInMonth = activeXgamingService.generateActiveWapInMonth(day)
    val totalVideoEntityInDay = liveStreamXgamingService.generateTotalVideoInDay(day)
    val totalWatcherEntityInDay = liveStreamXgamingService.generateTotalWatcherInDay(day)
    val totalViewEntityInDay = liveStreamXgamingService.generateTotalViewInDay(day)
    val maxViewEntityInDay = liveStreamXgamingService.generateMaxViewInDay(day)
    val totalStreamerEntityInDay = matchXgamingService.generateTotalStreamer(day)
    val totalChallengeEntityInDay = matchXgamingService.generateTotalChallenge(day)
    val totalChallengerEntityInDay = matchXgamingService.generateTotalChallenger(day)
    val totalSuccessEntityInDay = matchXgamingService.generateTotalSuccess(day)
    val hotStreamerEntityInDay = matchXgamingService.generateHotStreamer(day)

    /* report */
    val reportNewInstallInDay = deviceEntityInDay._reportNewInstallInDay
    val reportNewInstallInMonth = deviceEntityInMonth._reportNewInstallInMonth

    val reportActiveAppInDay = activeAppEntityInDay._reportAppInDay
    val reportActiveAppInMonth = activeAppEntityInMonth._reportAppInMonth
    val reportActiveWapInDay = activeWapEntityInDay._reportWapInDay
    val reportActiveWapInMonth = activeWapEntityInMonth._reportWapInMonth

    val reportTotalVideoInDay = totalVideoEntityInDay._reportTotalVideoInDay
    val reportTotalWatcherInDay = totalWatcherEntityInDay._reportTotalWatcherInDay
    val reportTotalViewInDay = totalViewEntityInDay._reportTotalViewInDay
    val reportMaxViewInDay = maxViewEntityInDay._reportMaxViewInDay

    val reportTotalStreamer = totalStreamerEntityInDay._reportTotalSteamerInDay
    val reportTotalChallenge = totalChallengeEntityInDay._reportTotalChallengeInDay
    val reportTotalChallenger = totalChallengerEntityInDay._reportTotalChallengerInDay
    val reportTotalSuccess = totalSuccessEntityInDay._reportTotalSuccessInDay
    val reportHotStreamer = hotStreamerEntityInDay._reportHotStreamerInDay

    /* metrics */
    val newInstallLoginInDay = deviceEntityInDay._newInstallLoginInDay
    val row = Row.fromSeq(Seq())
    val report = "Xgaming Report ".concat(day).concat(":\n")
      .concat(reportNewInstallInDay).concat("\n")
      .concat(reportNewInstallInMonth).concat("\n")
      .concat(reportActiveAppInDay).concat("\n")
      .concat(reportActiveAppInMonth).concat("\n")
      .concat(reportActiveWapInDay).concat("\n")
      .concat(reportActiveWapInMonth).concat("\n")
      .concat(reportTotalVideoInDay).concat("\n")
      .concat(reportTotalWatcherInDay).concat("\n")
      .concat(reportTotalViewInDay).concat("\n")
      .concat(reportMaxViewInDay).concat("\n")
      .concat(reportTotalStreamer).concat("\n")
      .concat(reportTotalChallenge).concat("\n")
      .concat(reportTotalChallenger).concat("\n")
      .concat(reportTotalSuccess).concat("\n")
      .concat(reportHotStreamer).concat("\n")
    report
  }
}
