package com.viettel.datalake.dao

import com.viettel.datalake.start.RunApplication
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.functions._

class RevenueXgamingDAO{
  val spark = RunApplication.spark
  object Model {
    val dateTime = "dateTime"
    val result = "result"
    val partnerID = "partnerID"
    val userID = "userID"
    val orderID = "orderID"
    val orderType = "orderType"
    val paymentType = "paymentType"
    val amount = "amount"
    val item = "item"
  }

  val schema = new StructType()
    .add(Model.dateTime, StringType,false)
    .add(Model.result, StringType, false)
    .add(Model.partnerID, LongType,false)
    .add(Model.userID, LongType,false)
    .add(Model.orderID, StringType, false )
    .add(Model.orderType, IntegerType, false)
    .add(Model.paymentType, IntegerType, false)
    .add(Model.amount, LongType,false)
    .add(Model.item, StringType, false)

  val dataDir = "D:\\data\\xgaming-revenue.txt"

  val df:DataFrame = load_data()

  def load_data(): DataFrame = {
    spark.read.option("delimiter","|").schema(schema).csv(dataDir)
  }

  def print_top(limit: Int): Unit ={
    df.take(limit).foreach(println)
  }

  def print_schema() : Unit = {
    df.printSchema()
  }

  def revenue_report(filterColumn: Column) :Long = {
    df.where(filterColumn)
      .agg(sum(col(Model.amount)))
      .collect()(0).getLong(0)
  }
}
