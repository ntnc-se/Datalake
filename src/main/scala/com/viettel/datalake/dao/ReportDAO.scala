package com.viettel.datalake.dao

import com.google.gson.Gson
import com.viettel.datalake.config.{GroupIdConfig, MochaConfig, NotificationConfig}
import com.viettel.datalake.entity.ReportEntity
import com.viettel.datalake.start.RunApplication
import com.viettel.datalake.utils.Helper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, to_date}

class ReportDAO {
  val spark = RunApplication.spark
  def reportDF(path: String): DataFrame = {
    var df: DataFrame = null
    try{
      df = spark.read.format("csv").option("header", "true").option("delimiter", "|").load(path)
      df
    } catch {
      case e: Exception => df
    }
  }
}
