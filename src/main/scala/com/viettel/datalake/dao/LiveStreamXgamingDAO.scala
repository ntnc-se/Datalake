package com.viettel.datalake.dao

import com.google.gson.Gson
import com.viettel.datalake.config.{GroupIdConfig, HadoopConfig, MochaConfig, NotificationConfig}
import com.viettel.datalake.entity.ReportEntity
import com.viettel.datalake.start.RunApplication
import com.viettel.datalake.utils.Helper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class LiveStreamXgamingDAO {
  val spark = RunApplication.spark
  val liveStreamDF = liveStreamXgamingDF()

  def liveStreamXgamingDF(): DataFrame = {
    val bYearMonth = Helper.beforDateTime("yyyy-MM")
    var df: DataFrame = null
    val path = HadoopConfig.XgamingVideoPath + "/*" + bYearMonth + "*"
    Helper.sendNotification("debug|" + path, "0969664623")
//    val path = "E:\\Data\\xgaming\\video"
    try{
      df = spark.read.format("csv").option("inferSchema", true).option("delimiter", "|").load(path)
      df = df.withColumnRenamed("_c0", "timestamp")
        .withColumnRenamed("_c1", "cdr")
        .withColumnRenamed("_c2", "channel")
        .withColumnRenamed("_c3", "source")
        .withColumnRenamed("_c4", "user_id")
        .withColumnRenamed("_c5", "is_vip")
        .withColumnRenamed("_c6", "client_type")
        .withColumnRenamed("_c7", "reversion")
        .withColumnRenamed("_c8", "network_type")
        .withColumnRenamed("_c9", "video_id")
        .withColumnRenamed("_c10", "state")
        .withColumnRenamed("_c11", "time_log")
        .withColumnRenamed("_c12", "lag_arr")
        .withColumnRenamed("_c13", "play_arr")
        .withColumnRenamed("_c14", "a")
        .withColumnRenamed("_c15", "average_lag")
        .withColumnRenamed("_c16", "average_watch")
        .withColumnRenamed("_c17", "xxx")
        .withColumnRenamed("_c18", "price")
        .withColumnRenamed("_c19", "ip_address")
        .withColumnRenamed("_c20", "user_agent")
        .withColumnRenamed("_c21", "media_link")
        .withColumnRenamed("_c22", "page_link")
        .withColumnRenamed("_c23", "e")
        .withColumnRenamed("_c24", "error_desc")
        .withColumnRenamed("_c25", "server_time")
        .withColumnRenamed("_c26", "volumne")
        .withColumnRenamed("_c27", "domain")
        .withColumnRenamed("_c28", "bandwith_array")
        .withColumnRenamed("_c29", "network_array")
        .withColumnRenamed("_c30", "is_ads")
        .withColumnRenamed("_c31", "recommand_type")
        .withColumnRenamed("_c32", "duration")
        .withColumnRenamed("_c33", "is_videolive")
        .withColumnRenamed("_c34", "cate_id")
        .withColumnRenamed("_c35", "game_id")
        .withColumnRenamed("_c36", "video_type")
        .withColumnRenamed("_c37", "streamer_id")
        .withColumnRenamed("_c38", "language_code")
        .withColumnRenamed("_c39", "country_code")
      df = df.withColumn("created_day", to_date(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"))
      df = df.withColumn("created_month", date_format(to_date(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"), "yyyy-MM"))
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
    Tổng số video livestream
   */
  def totalVideoInDay(day: String): Long = {
    val result = liveStreamDF.where((liveStreamDF("created_day") === day) && (liveStreamDF("is_videolive") === 1))
      .agg(countDistinct("video_id")).collect()(0).getLong(0)
    Helper.sendNotification("totalVideoInDay|" + result, "0969664623")
    result
  }

  /*
    Tổng số người xem (lọc trùng)
   */
  def totalWatcherInDay(day: String): Long = {
    liveStreamDF.where((liveStreamDF("created_day") === day) && (liveStreamDF("is_videolive") === 1)).show(1)
    val result = liveStreamDF.where((liveStreamDF("created_day") === day) && (liveStreamDF("is_videolive") === 1))
      .agg(countDistinct("user_id")).collect()(0).getLong(0)
    Helper.sendNotification("totalWatcherInDay|" + result, "0969664623")
    result
  }

  /*
    Tổng luot xem video
   */
  def totalWatch(day: String): Long = {
    val result = liveStreamDF.where((liveStreamDF("created_day") === day) && (liveStreamDF("is_videolive") === 1)).count()
    Helper.sendNotification("totalWatch|" + result, "0969664623")
    result
  }

  /*
    Lượt view đồng thời cao nhất
   */
  def maxView(day: String): Long = {
    val result = liveStreamDF.where((liveStreamDF("created_day") === day) && (liveStreamDF("is_videolive") === 1))
        .groupBy("video_id").count().orderBy(desc("count"))
        .collect()(0).getLong(1)
    Helper.sendNotification("maxView|" + result, "0969664623")
    result
  }
}
