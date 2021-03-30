package com.viettel.datalake.dao

import com.viettel.datalake.config.HadoopConfig
import com.viettel.datalake.start.RunApplication
import com.viettel.datalake.utils.Helper
import org.apache.spark.sql.DataFrame

//testv2
class UserMessageMusicRoomDAO{
  val spark = RunApplication.spark
  val musicRoomDF :DataFrame = userMessageMusicRoomDF()

  def userMessageMusicRoomDF(): DataFrame = {
    val bYearMonth = Helper.beforDateTime("yyyy-MM")
    var df: DataFrame = null
    val path = HadoopConfig.MochaUserMusicRoomPath + "/*" + bYearMonth + "*"
//    val path = "D:\\Document\\DataLake\\xgame\\log sample\\logActive.txt"
    try{
      df = spark.read.format("csv").option("delimiter", "|").load(path)
      return df
    } catch {
      case e: Exception => return df
    }
  }

  def test(): Unit = {
    println("zzzzzzzzz")
    musicRoomDF.show(1)
    println("xxxxxxxxxx")
  }
}
