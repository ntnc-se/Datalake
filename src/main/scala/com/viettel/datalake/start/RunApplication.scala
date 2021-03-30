package com.viettel.datalake.start

import com.google.gson.Gson
import com.viettel.bi.spark.ext.api.SparkCommand
import com.viettel.datalake.config.{GroupIdConfig, MochaConfig, NotificationConfig}
import com.viettel.datalake.entity.ReportEntity
import com.viettel.datalake.report.XgamingReport
import com.viettel.datalake.utils.Helper
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.dataformat.yaml.snakeyaml.util.UriEncoder

import java.io.{PrintWriter, StringWriter}
import scala.collection.mutable

object RunApplication extends SparkCommand{
  var configFile: String = null
  var spark: SparkSession = null

  def main(args: Array[String]): Unit = {
    println("zzz")
  }

  override def exec(sparkSession: SparkSession, args: Array[String], um: mutable.HashMap[String, Object]): Unit = {
    try{
      setSparkSession(sparkSession)
      setConfigFile(args)
      var day = Helper.beforDateTime("yyyy-MM-dd")
      if(args.length == 2) {
        day = args(1)
      }
      Helper.sendNotification("day|" + day, "0969664623")
      val report_ = XgamingReport.report(day)
      val response = Helper.sendReportV2(report_, GroupIdConfig.XgamingGroupId)
    } catch {
      case e: Exception => {
        val errors = new StringWriter()
        e.printStackTrace(new PrintWriter(errors))
        Helper.sendReport(errors.toString)
      }
    }

  }

  def setConfigFile(args: Array[String]): Unit = {
    this.configFile = args(0)
  }

  def setSparkSession(spark: SparkSession): Unit = {
    this.spark = spark
  }
}
