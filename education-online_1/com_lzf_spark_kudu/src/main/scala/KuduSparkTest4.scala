import java.sql.ResultSet
import java.{lang, util}

import _root_.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.client.KuduPredicate.ComparisonOp
import org.apache.kudu.client.{AsyncKuduClient, KuduClient, KuduPredicate}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object KuduSparkTest4 {
  private val groupid = "register_group_test"
  private val KUDU_MASTERS = "hadoop001,hadoop002,hadoop003"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.backpressure.enabled", "true")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sparkContext = ssc.sparkContext
    val topics = Array("register_topic")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop001:9092,hadoop002:9092,hadoop003:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1") //设置高可用地址
    sparkContext.hadoopConfiguration.set("dfs.nameservices", "nameservice1") //设置高可用地址
    val sqlProxy = new SqlProxy
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close() //关闭游标
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }
    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    val resultDStream = stream.mapPartitions(partitions => {
      partitions.map(item => {
        val line = item.value()
        val arr = line.split("\t")
        val id = arr(1)
        val app_name = id match {
          case "1" => "PC_1"
          case "2" => "APP_2"
          case _ => "Other_3"
        }
        (app_name, 1)
      })
    }).reduceByKey(_ + _)
    resultDStream.foreachRDD(rdd => {
      rdd.foreachPartition(parititon => {
        val kuduClient = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build //获取kudu连接
        val kuduTable = kuduClient.openTable("impala::default.register_table") //获取kudu table
        val schema = kuduTable.getSchema
        //根据当前表register_table  将相应需要查询的列放到arraylist中  表中有id servicetype count字段
        val projectColumns = new util.ArrayList[String]()
        projectColumns.add("id")
        projectColumns.add("servicetype")
        projectColumns.add("count")
        parititon.foreach(item => {
          var resultCount: Long = item._2 //声明结果值  默认为当前批次数据
          val appname = item._1.split("_")(0)
          val id = item._1.split("_")(1).toInt
          val eqPred = KuduPredicate.newComparisonPredicate(schema.getColumn("servicetype"),
            ComparisonOp.EQUAL, appname); //先根据设备名称过滤  过滤条件为等于app_name的数据
          val kuduScanner = kuduClient.newScannerBuilder(kuduTable).addPredicate(eqPred).build()
          while (kuduScanner.hasMoreRows) {
            val results = kuduScanner.nextRows()
            while (results.hasNext) {
              val result = results.next()
              //符合条件的数据有值则 和当前批次数据进行累加
              val count = result.getLong("count") //获取表中count值
              resultCount += count
            }
          }
          //最后将结果数据重新刷新到kudu
          val kuduSession = kuduClient.newSession()
          val upset = kuduTable.newUpsert() //调用upset 当主键存在数据进行修改操作 不存在则新增
          val row = upset.getRow
          row.addInt("id", id)
          row.addString("servicetype", appname)
          row.addLong("count", resultCount)
          kuduSession.apply(upset)
          kuduSession.close()
        })
        kuduClient.close()
      })
    })
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
