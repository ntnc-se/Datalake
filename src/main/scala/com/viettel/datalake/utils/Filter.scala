package com.viettel.datalake.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

abstract class Filter()

case class Equal(l: Filter, r: Filter) extends Filter
case class Field(n: String) extends Filter
case class Value(v:Any) extends Filter
case class NotEqual(l: Filter, r: Filter) extends Filter
case class And(l: Filter, r: Filter) extends Filter
case class Or(l: Filter, r: Filter) extends Filter

object Filter{
  def getFilterColumn(t: Filter): Column = t match {
    case Equal(l,r) => getFilterColumn(l) === getFilterColumn(r)
    case NotEqual(l,r) => getFilterColumn(l) =!= getFilterColumn(r)
    case Field(l) => col(l)
    case Value(v) => lit(v)
    case And(l,r) => getFilterColumn(l) and getFilterColumn(r)
    case Or(l,r) => getFilterColumn(l) or  getFilterColumn(r)
  }
}



