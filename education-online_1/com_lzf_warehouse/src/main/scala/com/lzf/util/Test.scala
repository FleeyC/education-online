package com.lzf.util

import org.apache.hadoop.mapred.{JobConf, Mapper, OutputCollector, Reporter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dwd_qz_controller").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = sparkSession.sparkContext
    val result = ssc.textFile("file://" + this.getClass.getResource("/1.txt").toURI.toString, 3)
    result.foreach(println)
    println(result.getNumPartitions)
//    result.saveAsTextFile("output1")
  }
}

