package com.lzf.sellcourse.bean

case class DwdSaleCourse(courseid: Int,
                         coursename: String,
                         status: String,
                         pointlistid: Int,
                         majorid: Int,
                         chapterid: Int,
                         chaptername: String,
                         edusubjectid: Int,
                         edusubjectname: String,
                         teacherid: Int,
                         teachername: String,
                         coursemanager: String,
                         money: java.math.BigDecimal,
                         dt: String,
                         dn: String,
                         rand_courseid: String)

case class DwdCourseShoppingCart(courseid: Int,
                                 orderid:String,
                                 coursename:String,
                                 cart_discount: java.math.BigDecimal,
                                 sellmoney: java.math.BigDecimal,
                                 cart_createtime: java.sql.Timestamp,
                                 dt: String,
                                 dn: String,
                                 rand_courseid: String)
