package com.lzf.hudi.test

import com.atguigu.bean.DwsMember
import com.atguigu.hudi.util.ParseJsonData
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object TableTypeTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("dwd_member_import")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    //    generateData(sparkSession)
    queryData(sparkSession)
    //    updateData(sparkSession)
  }


  def generateData(sparkSession: SparkSession) = {
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
      }).withColumn("ts", lit(commitTime))
      .withColumn("hudipartition", concat_ws("/", col("dt"), col("dn")))
    df.write.format("org.apache.hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL) //指定表类型为MERGE_ON_READ
      .option("hoodie.insert.shuffle.parallelism", 12)
      .option("hoodie.upsert.shuffle.parallelism", 12)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uid") //指定记录键
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts") //数据更新时间戳的
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "hudipartition") //hudi分区列
      .option("hoodie.table.name", "hudimembertest1") //hudi表名
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://hadoop101:10000") //hiveserver2地址
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default") //设置hudi与hive同步的数据库
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "member1") //设置hudi与hive同步的表名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt,dn") //hive表同步的分区列
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName) // 分区提取器 按/ 提取分区
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true") //设置数据集注册并同步到hive
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")
      .mode(SaveMode.Overwrite)
      .save("/user/atguigu/hudi/hudimembertest1")

    df.write.format("org.apache.hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL) //指定表类型为COPY_ON_WRITE
      .option("hoodie.insert.shuffle.parallelism", 12)
      .option("hoodie.upsert.shuffle.parallelism", 12)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uid") //指定记录键
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts") //数据更新时间戳的
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "hudipartition") //hudi分区列
      .option("hoodie.table.name", "hudimembertest2") //hudi表名
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://hadoop101:10000") //hiveserver2地址
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default") //设置hudi与hive同步的数据库
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "member2") //设置hudi与hive同步的表名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt,dn") //hive表同步的分区列
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName) // 分区提取器 按/ 提取分区
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true") //设置数据集注册并同步到hive
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")
      .mode(SaveMode.Overwrite)
      .save("/user/atguigu/hudi/hudimembertest2")
  }

  def queryData(sparkSession: SparkSession) = {
    val beginTime = 20200706134000l
    val df1 = sparkSession.read.format("org.apache.hudi")
      .load("/user/atguigu/hudi/hudimembertest1/*/*")
      .where("ts>1594013134644")

    //    val df2 = sparkSession.read.format("org.apache.hudi")
    //      .load("/user/atguigu/hudi/hudimembertest2/*/*").orderBy("uid")
    df1.show(100)
    //    df2.show()
    //    println("count1:" + df1.count())
    //    println(df2.count())
    //    println("count:"df2.count())
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
      }).where("uid>=0 and uid<=9")

    val result = df.map(item => {
      item.fullname = "testName"
      item
    }).withColumn("ts", lit(commitTime))
      .withColumn("hudipartition", concat_ws("/", col("dt"), col("dn")))
    //    result.show()
    result.write.format("org.apache.hudi")
      .option("hoodie.insert.shuffle.parallelism", 12)
      .option("hoodie.upsert.shuffle.parallelism", 12)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uid") //指定记录键
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts") //数据更新时间戳的
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "hudipartition") //hudi分区列
      .option("hoodie.table.name", "hudimembertest1") //hudi表名1
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://hadoop101:10000") //hiveserver2地址
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default") //设置hudi与hive同步的数据库
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "member1") //设置hudi与hive同步的表名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt,dn") //hive表同步的分区列
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName) // 分区提取器 按/ 提取分区
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true") //设置数据集注册并同步到hive
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")
      .mode(SaveMode.Append)
      .save("/user/atguigu/hudi/hudimembertest1")

    result.write.format("org.apache.hudi")
      .option("hoodie.insert.shuffle.parallelism", 12)
      .option("hoodie.upsert.shuffle.parallelism", 12)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uid") //指定记录键
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts") //数据更新时间戳的
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "hudipartition") //hudi分区列
      .option("hoodie.table.name", "hudimembertest2") //hudi表名
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://hadoop101:10000") //hiveserver2地址
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default") //设置hudi与hive同步的数据库
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "member2") //设置hudi与hive同步的表名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt,dn") //hive表同步的分区列
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName) // 分区提取器 按/ 提取分区
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true") //设置数据集注册并同步到hive
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")
      .mode(SaveMode.Append)
      .save("/user/atguigu/hudi/hudimembertest2")
  }
}
