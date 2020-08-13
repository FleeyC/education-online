package com.lzf.sellcourse.service

import com.alibaba.fastjson.JSONObject
import com.lzf.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object DwdSellCourseService {

  /**
    * 导入基本售课信息数据
    *
    * @param ssc
    * @param sparkSession
    */
  def importSaleCourseLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("/user/atguigu/ods/salecourse.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val courseid = jsonObject.getString("courseid")
        val coursename = jsonObject.getString("coursename")
        val status = jsonObject.getString("status")
        val pointlistid = jsonObject.getString("pointlistid")
        val majorid = jsonObject.getString("majorid")
        val chapterid = jsonObject.getString("chapterid")
        val chaptername = jsonObject.getString("chaptername")
        val edusubjectid = jsonObject.getString("edusubjectid")
        val edusubjectname = jsonObject.getString("edusubjectname")
        val teacherid = jsonObject.getString("teacherid")
        val teachername = jsonObject.getString("teachername")
        val coursemanager = jsonObject.getString("coursemanager")
        val money = jsonObject.getString("money")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (courseid, coursename, status, pointlistid, majorid, chapterid, chaptername,
          edusubjectid, edusubjectname, teacherid, teachername, coursemanager, money, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_sale_course")
  }

  /**
    * 导入课程支付信息
    *
    * @param ssc
    * @param sparkSession
    */
  def importCoursePay(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/atguigu/ods/coursepay.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val orderid = jsonObject.getString("orderid")
        val paymoney = jsonObject.getString("paymoney")
        val discount = jsonObject.getString("discount")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (orderid, discount, paymoney, createtime, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_course_pay")
  }

  def importCoursePay2(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/atguigu/ods/coursepay.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val orderid = jsonObject.getString("orderid")
        val paymoney = jsonObject.getString("paymoney")
        val discount = jsonObject.getString("discount")
        val createtime = jsonObject.getString("createitme")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (orderid, discount, paymoney, createtime, dt, dn)
      })
    }).toDF("orderid", "discount", "paymoney", "createtime", "dt", "dn").
      write.partitionBy("dt", "dn").
      bucketBy(10, "orderid").sortBy("orderid").
      mode(SaveMode.Append).saveAsTable("dwd.dwd_course_pay_cluster")
}

  def importCourseShoppingCart(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/atguigu/ods/courseshoppingcart.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val courseid = jsonObject.getString("courseid")
        val orderid = jsonObject.getString("orderid")
        val coursename = jsonObject.getString("coursename")
        val discount = jsonObject.getString("discount")
        val sellmoney = jsonObject.getString("sellmoney")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (courseid, orderid, coursename, discount, sellmoney, createtime, dt, dn)
      })
    }).toDF().coalesce(6).write.mode(SaveMode.Append).insertInto("dwd.dwd_course_shopping_cart")
  }

  def importCourseShoppingCart2(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/atguigu/ods/courseshoppingcart.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val courseid = jsonObject.getString("courseid")
        val orderid = jsonObject.getString("orderid")
        val coursename = jsonObject.getString("coursename")
        val discount = jsonObject.getString("discount")
        val sellmoney = jsonObject.getString("sellmoney")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (courseid, orderid, coursename, discount, sellmoney, createtime, dt, dn)
      })
    }).toDF("courseid", "orderid", "coursename", "discount", "sellmoney", "createtime", "dt", "dn")
      .write.partitionBy("dt", "dn").
      bucketBy(10, "orderid").sortBy("orderid").
      mode(SaveMode.Append).saveAsTable("dwd.dwd_course_shopping_cart_cluster")
  }
}
