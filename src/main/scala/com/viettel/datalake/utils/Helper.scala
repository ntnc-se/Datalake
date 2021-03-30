package com.viettel.datalake.utils

import com.viettel.datalake.config.{GroupIdConfig, MochaConfig, NotificationConfig}
import org.apache.commons.httpclient.methods.PostMethod
import org.apache.commons.httpclient.{HttpClient, MultiThreadedHttpConnectionManager}
import scalaj.http.Http

import java.text.SimpleDateFormat
import java.util.Calendar

object Helper {
  def beforDateTime(format: String): String = {
    val dateFormat = new SimpleDateFormat(format)
    val yesterday = Calendar.getInstance
    yesterday.add(Calendar.DATE, -1)
    val v_PreTable = dateFormat.format(yesterday.getTime())
    v_PreTable
  }

  def sendReport(report: String): String = {
    val json = Seq("username" -> MochaConfig.UserName,
    "pwd" -> MochaConfig.Password,
    "group_id" -> GroupIdConfig.ErrorGroupId,
    "content" -> report)
    val response = Http(MochaConfig.MochaApi).postForm(json).asString
    Helper.sendNotification("fuck_damge_code|" + response.code, "0969664623")
    response.body
  }

  def sendReportV2(report: String, groupId: String): Unit = {
    val connectionManager = new MultiThreadedHttpConnectionManager()
    val params = connectionManager.getParams();
    params.setConnectionTimeout(10000);
    params.setMaxTotalConnections(400);
    params.setDefaultMaxConnectionsPerHost(200);
    connectionManager.setParams(params)
    val httpclient = new HttpClient(connectionManager)
    Helper.sendNotification("debug_sendReportV2|" + MochaConfig.MochaApi, "0969664623")
    val postMethod = new PostMethod(MochaConfig.MochaApi)
    postMethod.setRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
    postMethod.addParameter("username", MochaConfig.UserName)
    postMethod.addParameter("pwd", MochaConfig.Password)
    postMethod.addParameter("from_msisdn", MochaConfig.FromMsisdn)
    postMethod.addParameter("group_id", groupId)
    postMethod.addParameter("content", report)
    httpclient.executeMethod(postMethod)
  }

  def sendNotification(notification: String, notiPhones: String): Unit = {
    val phones = notiPhones.split(",")
//    val content = UriEncoder.encode(notification)
    phones.foreach(phone => {
      val url = NotificationConfig.api.replace(":content", notification).replace(":phone", phone)
      Http(url).asString
    })
  }

  def ratio(now: Long, before: Long): Float = {
    if(before == 0) {
      0.0
    }
    val result = ((now - before)/before)*100
    val number = BigDecimal(result)
    number.setScale(2, BigDecimal.RoundingMode.HALF_UP).floatValue()
  }
}
