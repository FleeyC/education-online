package com.lzf.sellcourse.dao

import org.apache.spark.sql.SparkSession

object DwdSellCourseDao {

  def getDwdSaleCourse(sparkSession: SparkSession) = {
    sparkSession.sql("select courseid,coursename,status,pointlistid,majorid,chapterid,chaptername,edusubjectid," +
      "edusubjectname,teacherid,teachername,coursemanager,money,dt,dn from dwd.dwd_sale_course")
  }
  def getDwdCourseShoppingCart(sparkSession: SparkSession) = {
    sparkSession.sql("select courseid,orderid,coursename,discount,sellmoney,createtime,dt,dn from dwd.dwd_course_shopping_cart")
  }
  def getDwdCourseShoppingCart2(sparkSession: SparkSession) = {
    sparkSession.sql("select courseid,orderid,coursename,discount,sellmoney,createtime,dt,dn from dwd.dwd_course_shopping_cart_cluster")
  }

  def getDwdCoursePay(sparkSession: SparkSession) = {
    sparkSession.sql("select orderid,discount,paymoney,createtime,dt,dn from dwd.dwd_course_pay")
  }
  def getDwdCoursePay2(sparkSession: SparkSession) = {
    sparkSession.sql("select orderid,discount,paymoney,createtime,dt,dn from dwd.dwd_course_pay_cluster")
  }
}
