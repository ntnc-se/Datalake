package com.viettel.datalake.dao

import com.google.gson.Gson
import com.viettel.datalake.config.{GroupIdConfig, HadoopConfig, MochaConfig, NotificationConfig}
import com.viettel.datalake.entity.ReportEntity
import com.viettel.datalake.start.RunApplication
import com.viettel.datalake.utils.Helper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class MatchXgamingDAO {
  val spark = RunApplication.spark
  val matchDF :DataFrame = matchXgamingDF()

  def matchXgamingDF(): DataFrame = {
    val bYearMonth = Helper.beforDateTime("yyyy-MM")
    var df: DataFrame = null
    val path = HadoopConfig.XgamingMatchPath + "/*" + bYearMonth + "*"
    Helper.sendNotification("debug|" + path, "0969664623")
//    val path = "E:\\Data\\xgaming\\match"
    try{
      df = spark.read.format("csv").option("inferSchema", true).option("delimiter", "|").load(path)
      df = df.withColumnRenamed("_c0", "timestamp")
          .withColumnRenamed("_c1", "streamer_id")
          .withColumnRenamed("_c2", "total_chanllenger")
          .withColumnRenamed("_c3", "status")
          .withColumnRenamed("_c4", "created_at")
          .withColumnRenamed("_c5", "match_id")
          .withColumnRenamed("_c6", "streamer_name")
      df = df.withColumn("created_day", to_date(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
      df = df.withColumn("created_month", date_format(to_date(col("timestamp"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM"))
      df
    } catch {
      case e: Exception => {
        val warning = NotificationConfig.MissLog.replace(":log", "match_xgaming")
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
      Tổng số streamer tạo thách đấu
   */
  def totalStreamer(day: String): Long = {
    if(matchDF == null) {
      return 0
    }
    val result = matchDF.where(matchDF("created_day") === day).agg(countDistinct("streamer_id")).collect()(0).getLong(0)
    Helper.sendNotification("totalStreamer|" + result, "0969664623")
    result
  }

  /*
    Tổng số thách đấu được tạo
   */
  def totalMatch(day: String): Long = {
    if(matchDF == null) {
      return 0
    }
    val result = matchDF.where(matchDF("created_day") === day).count()
    Helper.sendNotification("totalMatch|" + result, "0969664623")
    result
  }

  /*
    Tổng số user tham gia thách đấu
   */
  def totalChanllenger(day: String): Long = {
    if(matchDF == null) {
      return 0
    }
    val result = matchDF.where(matchDF("created_day") === day).agg(sum("total_chanllenger")).collect()(0).getLong(0)
    Helper.sendNotification("totalChanllenger|" + result, "0969664623")
    result
  }

  /*
    Tổng số trận thách đấu được map thành công
   */
  def totalSuccess(day: String): Long = {
    if(matchDF == null) {
      return 0
    }
    val result = matchDF.where((matchDF("created_day") === day) && ((matchDF("status") >= 3) && (matchDF("status") =!= 7)))
      .agg(countDistinct("match_id")).collect()(0).getLong(0)
    Helper.sendNotification("totalSuccess|" + result, "0969664623")
    result
  }

  /*
    Trận có nhiều user ấn tham gia thách đấu nhất
   */
  def hotStreamer(day: String): String = {
    if(matchDF == null) {
      return ""
    }
    val result = matchDF.where(matchDF("created_day") === day)
      .groupBy("streamer_name").agg(sum("total_chanllenger").as("sum_chanllenger"))
      .orderBy(desc("sum_chanllenger")).select("streamer_name").limit(1)
      .collect()(0).getString(0)
    Helper.sendNotification("hotStreamer|" + result, "0969664623")
    result
  }

}
