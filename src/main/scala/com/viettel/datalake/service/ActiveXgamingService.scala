package com.viettel.datalake.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.viettel.datalake.config.{HadoopConfig, ReportConfig}
import com.viettel.datalake.dao.{ActiveXgamingDAO, ReportDAO}
import com.viettel.datalake.entity.ActiveEntity
import com.viettel.datalake.utils.Helper

class ActiveXgamingService {
  val activeXgamingDAO = new ActiveXgamingDAO
  val reportDAO = new ReportDAO
  val activeEntity = new ActiveEntity
  /*
    Active App
   */
  def generateActiveAppInDay(day: String): ActiveEntity = {
    var ratioActiveAppLoginInDay:Float = 0
    var bActiveAppLoginInDay: Long = 0
    var ratioActiveAppNoLoginInDay:Float = 0
    var bActiveAppNoLoginInDay: Long = 0
    var ratioActiveTotalInDay:Float = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusDays(1).toString.replace("-","").substring(0, 8)

    val activeAppLoginInDay = activeXgamingDAO.activeAppLoginInDay(day)
    activeEntity._activeAppLoginInDay = activeAppLoginInDay
    val activeAppNoLoginInDay = activeXgamingDAO.activeAppNoLoginInDay(day)
    activeEntity._activeAppNoLoginInDay = activeAppNoLoginInDay
    val activeAppTotalInDay = activeAppLoginInDay + activeAppNoLoginInDay
    activeEntity._activeAppTotalInDay = activeAppTotalInDay

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      val bDF = bReport.select(bReport("active_app_login_in_day"), bReport("active_app_nologin_in_day"), bReport("active_app_total_in_day"))
        .where(bReport("createdday") === bDay).collect()
      ratioActiveAppLoginInDay = Helper.ratio(activeAppLoginInDay, bDF(0).getLong(0))
      ratioActiveAppNoLoginInDay = Helper.ratio(activeAppNoLoginInDay, bDF(0).getLong(1))
      ratioActiveTotalInDay = Helper.ratio(activeAppTotalInDay, bDF(0).getLong(2))
    }

    val report = ReportConfig.XgamingUserActiveAppInDay.replace(":login", activeAppLoginInDay.toString)
      .replace(":ratio_login", ratioActiveAppLoginInDay.toString)
      .replace(":nologin", activeAppNoLoginInDay.toString)
      .replace(":ratio_nologin", ratioActiveAppNoLoginInDay.toString)
      .replace(":total", activeAppTotalInDay.toString)
      .replace(":ratio_total", ratioActiveTotalInDay.toString)
    Helper.sendNotification(report, "0969664623")
    activeEntity._reportAppInDay = report
    activeEntity
  }

  def generateActiveAppInMonth(day: String): ActiveEntity = {
    var ratioActiveAppLoginInMonth:Float = 0
    var ratioActiveAppNoLoginInMonth:Float = 0
    var bActiveAppNoLoginInMonth: Long = 0
    var ratioActiveAppTotalInDay: Float = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusMonths(1).toString.replace("-","").substring(0, 8)
    val month = day.substring(0, 7)

    val activeAppLoginInMonth = activeXgamingDAO.activeAppLoginInMonth(month)
    activeEntity._activeAppLoginInMonth = activeAppLoginInMonth
    val activeAppNoLoginInMonth = activeXgamingDAO.activeAppNoLoginInMonth(month)
    activeEntity._activeAppNoLoginInMonth = activeAppNoLoginInMonth
    val activeAppTotalInMonth = activeAppLoginInMonth + activeAppNoLoginInMonth
    activeEntity._activeAppTotalInMonth = activeAppTotalInMonth

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      val bDF = bReport.select(bReport("active_app_login_in_month"), bReport("active_app_nologin_in_month"), bReport("active_app_total_in_month"))
        .where(bReport("createdday") === bDay).collect()
      ratioActiveAppLoginInMonth = Helper.ratio(activeAppLoginInMonth, bDF(0).getLong(0))
      ratioActiveAppNoLoginInMonth = Helper.ratio(activeAppNoLoginInMonth, bDF(0).getLong(1))
      ratioActiveAppTotalInDay = Helper.ratio(activeAppTotalInMonth, bDF(0).getLong(2))
    }

    val report = ReportConfig.XgamingUserActiveAppInMonth.replace(":login", activeAppLoginInMonth.toString)
      .replace(":ratio_login", ratioActiveAppLoginInMonth.toString)
      .replace(":nologin", activeAppNoLoginInMonth.toString)
      .replace(":ratio_nologin", ratioActiveAppNoLoginInMonth.toString)
      .replace(":total", activeAppTotalInMonth.toString)
      .replace(":ratio_total", ratioActiveAppTotalInDay.toString)
    Helper.sendNotification(report, "0969664623")
    activeEntity._reportAppInMonth = report
    activeEntity
  }

  /*
    Active WEB/WAP
   */
  def generateActiveWapInDay(day: String): ActiveEntity = {
    var ratioActiveWapLoginInDay:Float = 0
    var ratioActiveWapNoLoginInDay:Float = 0
    var bActiveWapNoLoginInDay: Long = 0
    var ratioActiveWapTotalInDay: Float = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusDays(1).toString.replace("-","").substring(0, 8)

    val activeWapLoginInDay = activeXgamingDAO.activeWapLoginInDay(day)
    activeEntity._activeWapLoginInDay = activeWapLoginInDay
    val activeWapNoLoginInDay = activeXgamingDAO.activeWapNoLoginInDay(day)
    activeEntity._activeWapNoLoginInDay = activeWapNoLoginInDay
    val activeWapTotalInDay = activeWapLoginInDay + activeWapNoLoginInDay
    activeEntity._activeWapTotalInDay = activeWapTotalInDay

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      val bDF = bReport.select(bReport("active_wap_login_in_day"), bReport("active_wap_nologin_in_day"), bReport("active_wap_total_in_day"))
        .where(bReport("createdday") === bDay).collect()
      ratioActiveWapLoginInDay = Helper.ratio(activeWapLoginInDay, bDF(0).getLong(0))
      ratioActiveWapNoLoginInDay = Helper.ratio(activeWapNoLoginInDay, bDF(0).getLong(1))
      ratioActiveWapTotalInDay = Helper.ratio(activeWapTotalInDay, bDF(0).getLong(2))
    }

    val report = ReportConfig.XgamingUserActiveWapInDay.replace(":login", activeWapLoginInDay.toString)
      .replace(":ratio_login", ratioActiveWapLoginInDay.toString)
      .replace(":nologin", activeWapNoLoginInDay.toString)
      .replace(":ratio_nologin", ratioActiveWapNoLoginInDay.toString)
      .replace(":total", activeWapTotalInDay.toString)
      .replace(":ratio_total", ratioActiveWapTotalInDay.toString)
    Helper.sendNotification(report, "0969664623")
    activeEntity._reportWapInDay = report
    activeEntity
  }

  def generateActiveWapInMonth(day: String): ActiveEntity = {
    var ratioActiveWapLoginInMonth:Float = 0
    var ratioActiveWapNoLoginInMonth:Float = 0
    var bActiveWapNoLoginInMonth: Long = 0
    var ratioActiveWapTotalInMonth:Float = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusMonths(1).toString.replace("-","").substring(0, 8)
    val month = day.substring(0, 7)

    val activeWapLoginInMonth = activeXgamingDAO.activeWapLoginInMonth(month)
    activeEntity._activeWapLoginInMonth = activeWapLoginInMonth
    val activeWapNoLoginInMonth = activeXgamingDAO.activeAppNoLoginInMonth(month)
    activeEntity._activeWapNoLoginInMonth = activeWapNoLoginInMonth
    val activeWapTotalInMonth = activeWapLoginInMonth + activeWapNoLoginInMonth
    activeEntity._activeWapTotalInMonth = activeWapTotalInMonth

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      val bDF = bReport.select(bReport("active_wap_login_in_month"), bReport("active_wap_nologin_in_month"), bReport("active_wap_total_in_month"))
        .where(bReport("createdday") === bDay).collect()
      ratioActiveWapLoginInMonth = Helper.ratio(activeWapLoginInMonth, bDF(0).getLong(0))
      ratioActiveWapNoLoginInMonth = Helper.ratio(activeWapNoLoginInMonth, bDF(0).getLong(1))
      ratioActiveWapTotalInMonth = Helper.ratio(activeWapTotalInMonth, bDF(0).getLong(1))
    }

    val report = ReportConfig.XgamingUserActiveWapInMonth.replace(":login", activeWapLoginInMonth.toString)
      .replace(":ratio_login", ratioActiveWapLoginInMonth.toString)
      .replace(":nologin", activeWapNoLoginInMonth.toString)
      .replace(":ratio_nologin", ratioActiveWapNoLoginInMonth.toString)
      .replace(":total", activeWapTotalInMonth.toString)
      .replace(":ratio_total", ratioActiveWapTotalInMonth.toString)
    Helper.sendNotification(report, "0969664623")
    activeEntity._reportWapInMonth = report
    activeEntity
  }
}
