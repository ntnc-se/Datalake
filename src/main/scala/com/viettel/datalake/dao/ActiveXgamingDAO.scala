package com.viettel.datalake.dao

import com.google.gson.Gson
import com.viettel.datalake.config.{GroupIdConfig, HadoopConfig, MochaConfig, NotificationConfig}
import com.viettel.datalake.entity.ReportEntity
import com.viettel.datalake.start.RunApplication
import com.viettel.datalake.utils.Helper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class ActiveXgamingDAO {
  val spark = RunApplication.spark
  val activeDF :DataFrame = activeXgamingDF()

  def activeXgamingDF(): DataFrame = {
    val bYearMonth = Helper.beforDateTime("yyyy-MM")
    var df: DataFrame = null
    val path = HadoopConfig.XgamingActivePath + "/*" + bYearMonth + "*"
    Helper.sendNotification("debug|" + path, "0969664623")
//    val path = "E:\\Data\\xgaming\\active"
    try{
      df = spark.read.format("csv").option("inferSchema", true).option("delimiter", "|").load(path)
      df = df.withColumnRenamed("_c0", "timestamp")
          .withColumnRenamed("_c1", "user_id")
          .withColumnRenamed("_c2", "device_id")
          .withColumnRenamed("_c3", "action")
          .withColumnRenamed("_c4", "type")
      df = df.withColumn("created_day", to_date(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
      df = df.withColumn("created_month", date_format(to_date(col("timestamp"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM"))
      df
    } catch {
      case e: Exception => {
        val warning = NotificationConfig.MissLog.replace(":log", "device_xgaming")
                                                .replace(":user", "thongnh")
        val report = new ReportEntity(MochaConfig.UserName, MochaConfig.Password, MochaConfig.FromMsisdn, GroupIdConfig.ErrorGroupId)
        report.content = warning
        val warningReport = new Gson().toJson(report)
        Helper.sendReport(warningReport)
        df
      }
    }
  }

  /*
      Active App
   */
  def activeAppLoginInDay(day: String): Long = {
    if(activeDF == null) {
      return 0
    }
    val result = activeDF.where((activeDF("created_day") === day) && (activeDF("user_id") =!= 0) && (activeDF("type") === "APP"))
      .agg(countDistinct("user_id")).collect()(0).getLong(0)
    result
  }

  def activeAppNoLoginInDay(day: String): Long = {
    if(activeDF == null) {
      return 0
    }
    val result = activeDF.where((activeDF("created_day") === day) && (activeDF("user_id") === 0) && (activeDF("type") === "APP"))
      .agg(countDistinct("device_id")).collect()(0).getLong(0)
    result
  }

  def activeAppLoginInMonth(month: String): Long = {
    if(activeDF == null) {
      return 0
    }
    val result = activeDF.where((activeDF("created_month") === month) && (activeDF("user_id") =!= 0) && (activeDF("type") === "APP"))
      .agg(countDistinct("user_id")).collect()(0).getLong(0)
    result
  }

  def activeAppNoLoginInMonth(month: String): Long = {
    if(activeDF == null) {
      return 0
    }
    val result = activeDF.where((activeDF("created_month") === month) && (activeDF("user_id") === 0) && (activeDF("type") === "APP"))
      .agg(countDistinct("device_id")).collect()(0).getLong(0)
    result
  }

  /*
    Active WAP/WEB
   */
  def activeWapLoginInDay(day: String): Long = {
    if(activeDF == null) {
      return 0
    }
    val result = activeDF.where((activeDF("created_day") === day) && (activeDF("user_id") =!= 0))
      .where((activeDF("type") === "WAP") || (activeDF("type") === "WEB"))
      .agg(countDistinct("user_id")).collect()(0).getLong(0)
    result
  }

  def activeWapNoLoginInDay(day: String): Long = {
    if(activeDF == null) {
      return 0
    }
    val result = activeDF.where((activeDF("created_day") === day) && (activeDF("user_id") === 0))
      .where((activeDF("type") === "WAP") || (activeDF("type") === "WEB"))
      .agg(countDistinct("device_id")).collect()(0).getLong(0)
    result
  }

  def activeWapLoginInMonth(month: String): Long = {
    if(activeDF == null) {
      return 0
    }
    val result = activeDF.where((activeDF("created_month") === month) && (activeDF("user_id") =!= 0))
      .where((activeDF("type") === "WAP") || (activeDF("type") === "WEB"))
      .agg(countDistinct("user_id")).collect()(0).getLong(0)
    result
  }

  def activeWapNoLoginInMonth(month: String): Long = {
    if(activeDF == null) {
      return 0
    }
    val result = activeDF.where((activeDF("created_month") === month) && (activeDF("user_id") === 0))
      .where((activeDF("type") === "WAP") || (activeDF("type") === "WEB"))
      .agg(countDistinct("device_id")).collect()(0).getLong(0)
    result
  }
}
