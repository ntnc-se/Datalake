package com.viettel.datalake.dao

import com.google.gson.Gson
import com.viettel.datalake.config.{GroupIdConfig, HadoopConfig, MochaConfig, NotificationConfig}
import com.viettel.datalake.entity.ReportEntity
import com.viettel.datalake.start.RunApplication
import com.viettel.datalake.utils.Helper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


class DeviceXgamingDAO {
  val spark = RunApplication.spark
  val deviceDF :DataFrame = deviceXgamingDF()

  def deviceXgamingDF(): DataFrame = {
    val bYearMonth = Helper.beforDateTime("yyyy-MM");
    var df: DataFrame = null
    val path = HadoopConfig.XgamingDevicePath + "/*" + bYearMonth + "*"
    Helper.sendNotification("debug|" + path, "0969664623")
    //    val path = "E:\\Data\\xgaming\\device"
    df = spark.read.format("csv").option("inferSchema", true).option("delimiter", "|").load(path)
    df = df.withColumnRenamed("_c0", "timestamp")
      .withColumnRenamed("_c1", "user_id")
      .withColumnRenamed("_c2", "client_type")
      .withColumnRenamed("_c3", "device_id")
      .withColumnRenamed("_c4", "created_at")
    df = df.withColumn("created_day", to_date(col("created_at"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("created_month", date_format(to_date(col("created_at"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM"))
    df

  }

  /*
    New Device install in day
   */
  def newInstallInDayLogin(day: String): Long = {
    if(deviceDF == null) {
      return 0
    }
    val result = deviceDF.where((deviceDF("created_day") === day) && (deviceDF("user_id") =!= 0))
      .agg(countDistinct("device_id")).collect()(0).getLong(0)
    Helper.sendNotification("newInstallInDayLogin|" + result.toString, "0969664623")
    result
  }

  def newInstallInDayNoLogin(day: String): Long = {
    if(deviceDF == null) {
      return 0
    }
    val result = deviceDF.where((deviceDF("created_day") === day) && (deviceDF("user_id") === 0))
      .agg(countDistinct("device_id")).collect()(0).getLong(0)
    result
  }

  /*
    New Device install in month
   */
  def newInstallLoginInMonth(month: String): Long = {
    if(deviceDF == null) {
      return 0
    }
    val result = deviceDF.where((deviceDF("created_month") === month) && (deviceDF("user_id") =!= 0))
      .agg(countDistinct("device_id"))
      .collect()(0).getLong(0)
    result
  }

  def newInstallNoLoginInMonth(month: String): Long = {
    if(deviceDF == null) {
      return 0
    }
    val result = deviceDF.where((deviceDF("created_month") === month) && (deviceDF("user_id") === 0))
      .agg(countDistinct("device_id"))
      .collect()(0).getLong(0)
    result
  }
}
