package com.lzf.hudi.test

import com.atguigu.bean.DwsMember
import com.atguigu.hudi.util.ParseJsonData
import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.{SaveMode, SparkSession}

object HoodieDataSourceExample {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("dwd_member_import")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    //    insertData(sparkSession)
    //    queryData(sparkSession) //count:94175
//    updateData(sparkSession)
    //    incrementalQuery(sparkSession)
        pointInTimeQuery(sparkSession)
  }

  /**
    * 读取hdfs日志文件通过hudi写入hdfs
    *
    * @param sparkSession
    */
  def insertData(sparkSession: SparkSession) = {
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    val commitTime = System.currentTimeMillis().toString //生成提交时间
    val df = sparkSession.read.text("/user/atguigu/ods/member.log")
      .mapPartitions(partitions => {
        partitions.map(item => {
          val jsonObject = ParseJsonData.getJsonData(item.getString(0))
          DwsMember(jsonObject.getIntValue("uid"), jsonObject.getIntValue("ad_id")
            , jsonObject.getString("fullname"), jsonObject.getString("iconurl")
            , jsonObject.getString("dt"), jsonObject.getString("dn"))
        })
      })
    val result = df.withColumn("ts", lit(commitTime)) //添加ts 时间戳列
      .withColumn("uuid", col("uid")) //添加uuid 列 如果数据中uuid相同hudi会进行去重
      .withColumn("hudipartition", concat_ws("/", col("dt"), col("dn"))) //增加hudi分区列
    result.write.format("org.apache.hudi")
      //      .options(org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs)
      .option("hoodie.insert.shuffle.parallelism", 12)
      .option("hoodie.upsert.shuffle.parallelism", 12)
      .option("PRECOMBINE_FIELD_OPT_KEY", "ts") //指定提交时间列
      .option("RECORDKEY_FIELD_OPT_KEY", "uuid") //指定uuid唯一标示列
      .option("hoodie.table.name", "testTable")
      .option("hoodie.datasource.write.partitionpath.field", "hudipartition") //分区列
      .mode(SaveMode.Overwrite)
      .save("/user/atguigu/hudi")
  }

  /**
    * 查询hdfs上的hudi数据
    *
    * @param sparkSession
    */
  def queryData(sparkSession: SparkSession) = {
    val df = sparkSession.read.format("org.apache.hudi")
      .load("/user/atguigu/hudi/*/*")
    df.show()
    println("count:" + df.count())
  }

  def updateData(sparkSession: SparkSession) = {
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    val commitTime = System.currentTimeMillis().toString //生成提交时间
    val df = sparkSession.read.text("/user/atguigu/ods/member.log")
      .mapPartitions(partitions => {
        partitions.map(item => {
          val jsonObject = ParseJsonData.getJsonData(item.getString(0))
          DwsMember(jsonObject.getIntValue("uid"), jsonObject.getIntValue("ad_id")
            , jsonObject.getString("fullname"), jsonObject.getString("iconurl")
            , jsonObject.getString("dt"), jsonObject.getString("dn"))
        })
      }).limit(100)
    val result = df.withColumn("ts", lit(commitTime)) //添加ts 时间戳列
      .withColumn("uuid", col("uid")) //添加uuid 列 如果数据中uuid相同hudi会进行去重
      .withColumn("hudipartition", concat_ws("/", col("dt"), col("dn"))) //增加hudi分区列
    result.write.format("org.apache.hudi")
      //      .options(org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs)
      .option("hoodie.insert.shuffle.parallelism", 12)
      .option("hoodie.upsert.shuffle.parallelism", 12)
      .option("PRECOMBINE_FIELD_OPT_KEY", "ts") //指定提交时间列
      .option("RECORDKEY_FIELD_OPT_KEY", "uuid") //指定uuid唯一标示列
      .option("hoodie.table.name", "testTable")
      .option("hoodie.datasource.write.partitionpath.field", "hudipartition") //分区列
      .mode(SaveMode.Append)
      .save("/user/atguigu/hudi")
  }

  def incrementalQuery(sparkSession: SparkSession) = {
    val beginTime = 20200703142353l
    val df = sparkSession.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL) //指定模式为增量查询
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, beginTime) //设置开始查询的时间戳  不需要设置结束时间戳
      .load("/user/atguigu/hudi")
    df.show()
    println(df.count())
  }

  def pointInTimeQuery(sparkSession: SparkSession) = {
    val beginTime = 20200703150000l
    val endTime = 20200703160000l
    val df = sparkSession.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL) //指定模式为增量查询
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, beginTime) //设置开始查询的时间戳
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, endTime)
      .load("/user/atguigu/hudi")
    df.show()
    println(df.count())

  }

}
