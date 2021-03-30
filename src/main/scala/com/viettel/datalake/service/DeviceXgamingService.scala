package com.viettel.datalake.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.viettel.datalake.config.{HadoopConfig, ReportConfig}
import com.viettel.datalake.dao.{DeviceXgamingDAO, ReportDAO}
import com.viettel.datalake.entity.DeviceEntity
import com.viettel.datalake.utils.Helper
import org.apache.spark.sql.DataFrame

class DeviceXgamingService {
  val deviceEntity = new DeviceEntity
  val deviceXgamingDAO = new DeviceXgamingDAO
  val reportDAO = new ReportDAO
  
  def generateNewInstallInDay(day: String): DeviceEntity = {
    var ratioNewInstallLoginInDay:Float = 0
    var bNewInstallLoginInDay: Long = 0
    var ratioNewInstallNoLoginInDay:Float = 0
    var bNewInstallNoLoginInDay: Long = 0
    var ratioNewInstallTotal:Float = 0


    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusDays(1).toString.replace("-","").substring(0, 8)

    val newInstallLoginInDay = deviceXgamingDAO.newInstallInDayLogin(day)
    deviceEntity._newInstallLoginInDay = newInstallLoginInDay
    val newInstallNoLoginInDay = deviceXgamingDAO.newInstallInDayNoLogin(day)
    deviceEntity._newInstallNoLoginInDay = newInstallNoLoginInDay
    val newInstallTotalInDay = newInstallLoginInDay + newInstallNoLoginInDay
    deviceEntity._newInstallTotalInDay = newInstallTotalInDay
    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)

    if(bReport != null) {
      val bDF = bReport.select(bReport("new_install_login_in_day"), bReport("new_install_nologin_in_day"), bReport("new_install_total_in_day"))
        .where(bReport("createdday") === bDay).collect()
      ratioNewInstallLoginInDay = Helper.ratio(newInstallLoginInDay, bDF(0).getLong(0))
      ratioNewInstallNoLoginInDay = Helper.ratio(newInstallNoLoginInDay, bDF(0).getLong(1))
      ratioNewInstallTotal = Helper.ratio(newInstallTotalInDay, bDF(0).getLong(2))
    }
    Helper.sendNotification("bReport|FALSE", "0969664623")
    val report = ReportConfig.XgamingUserInstallInDay.replace(":login", newInstallLoginInDay.toString)
      .replace(":ratio_login", ratioNewInstallLoginInDay.toString)
      .replace(":nologin", newInstallNoLoginInDay.toString)
      .replace(":ratio_nologin", ratioNewInstallNoLoginInDay.toString)
      .replace(":total", newInstallTotalInDay.toString)
      .replace(":ratio_total", ratioNewInstallTotal.toString)
    Helper.sendNotification(report, "0969664623")
    deviceEntity._reportNewInstallInDay = report
    deviceEntity
  }

  def generateNewInstallInMonth(day: String): DeviceEntity = {
    var ratioNewInstallLoginInMonth:Float = 0
    var bNewInstallLoginInMonth: Long = 0
    var ratioNewInstallNoLoginInMonth:Float = 0
    var bNewInstallNoLoginInMonth: Long = 0
    var ratioNewInstallTotal:Float = 0

    val _day = day + " 00:00:00"
    val now = LocalDateTime.parse(_day, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val bDay = now.minusDays(1).toString.replace("-","").substring(0, 8)
    val month = day.substring(0, 7)

    val newInstallLoginInMonth = deviceXgamingDAO.newInstallLoginInMonth(month)
    deviceEntity._newInstallLoginInMonth = newInstallLoginInMonth
    val newInstallNoLoginInMonth = deviceXgamingDAO.newInstallNoLoginInMonth(month)
    deviceEntity._newInstallNoLoginInMonth = newInstallNoLoginInMonth
    val newInstallTotalInMonth = newInstallLoginInMonth + newInstallNoLoginInMonth
    deviceEntity._newInstallTotalInMonth = newInstallTotalInMonth

    val bReport = reportDAO.reportDF(HadoopConfig.XgamingReportPath)
    if(bReport != null) {
      val bDF = bReport.select(bReport("new_install_login_in_month"), bReport("new_install_nologin_in_month"), bReport("new_install_total_in_month"))
        .where(bReport("createdday") === bDay).collect()
      ratioNewInstallLoginInMonth = Helper.ratio(newInstallLoginInMonth, bDF(0).getLong(0))
      ratioNewInstallNoLoginInMonth = Helper.ratio(newInstallNoLoginInMonth, bDF(0).getLong(1))
      ratioNewInstallTotal = Helper.ratio(newInstallTotalInMonth, bDF(0).getLong(2))
    }

    val report = ReportConfig.XgamingUserInstallInMonth.replace(":login", newInstallLoginInMonth.toString)
      .replace(":ratio_login", ratioNewInstallLoginInMonth.toString)
      .replace(":nologin", newInstallNoLoginInMonth.toString)
      .replace(":ratio_nologin", ratioNewInstallNoLoginInMonth.toString)
      .replace(":total", newInstallTotalInMonth.toString)
      .replace(":ratio_total", ratioNewInstallTotal.toString)
    deviceEntity._reportNewInstallInMonth = report
    deviceEntity
  }
}
