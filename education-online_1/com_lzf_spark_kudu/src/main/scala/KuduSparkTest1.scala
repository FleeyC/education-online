import java.util

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//创建kudu表
object KuduSparkTest1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkConetxt = spark.sparkContext
    val kuduContext = new KuduContext("hadoop001,hadoop002,hadoop003", sparkConetxt)
    val tableFields = (Array(new StructField("id", IntegerType, false), new StructField("name", StringType)))
    val arrayList = new util.ArrayList[String]()
    arrayList.add("id")
    val b = new CreateTableOptions().setNumReplicas(1).addHashPartitions(arrayList, 3)
    kuduContext.createTable("test_table", StructType(tableFields), Seq("id"), b)
  }
}
