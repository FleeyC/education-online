package com.lzf.qzpoint.streaming

import java.lang
import java.sql.{Connection, ResultSet}
import java.text.NumberFormat

import com.lzf.qzpoint.util.{DataSourceUtil, ParseJsonData, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkFiles}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 页面转换率实时统计
  *
  * nohup spark-submit  --master yarn --deploy-mode client --driver-memory 1g --num-executors 5 --executor-cores 2 --executor-memory 2g --class com.lzf.qzpoint.streaming.PageStreaming  --queue spark com_lzf_sparkstreaming-1.0-SNAPSHOT-jar-with-dependencies.jar >1.txt>&1 &
  */
object PageStreaming {
  private val groupid = "page_groupid"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    //          .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val topics = Array("page_topic")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //查询mysql中是否存在偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select *from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close()
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    //设置kafka消费数据的参数 判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    //解析json数据
    val dsStream = stream.map(item => item.value()).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val uid = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
        val app_id = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
        val device_id = if (jsonObject.containsKey("device_id")) jsonObject.getString("device_id") else ""
        val ip = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
        val last_page_id = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
        val pageid = if (jsonObject.containsKey("page_id")) jsonObject.getString("page_id") else ""
        val next_page_id = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
        (uid, app_id, device_id, ip, last_page_id, pageid, next_page_id)
      })
    })
    dsStream.cache()
    val pageValueDStream = dsStream.map(item => (item._5 + "_" + item._6 + "_" + item._7, 1))
    val resultDStream = pageValueDStream.reduceByKey(_ + _)
    resultDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //在分区下获取jdbc连接
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partition.foreach(item => {
            calcPageJumpCount(sqlProxy, item, client) //计算页面跳转个数
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
    })
    ssc.sparkContext.addFile("hdfs://mycluster/user/sparkstreaming/ip2region.db")
    //        ssc.sparkContext.addFile(this.getClass.getResource("/ip2region.db").getPath) //广播文件
    val ipDStream = dsStream.mapPartitions(patitions => {
      val dbFile = SparkFiles.get("ip2region.db")
      val ipsearch = new DbSearcher(new DbConfig(), dbFile)
      patitions.map { item =>
        val ip = item._4
        val province = ipsearch.memorySearch(ip).getRegion().split("\\|")(2) //获取ip详情   中国|0|上海|上海市|有线通
        (province, 1l) //根据省份 统计点击个数
      }
    }).reduceByKey(_ + _)


    ipDStream.foreachRDD(rdd => {
      //查询mysql历史数据 转成rdd
      val ipSqlProxy = new SqlProxy()
      val ipClient = DataSourceUtil.getConnection
      try {
        val history_data = new ArrayBuffer[(String, Long)]()
        ipSqlProxy.executeQuery(ipClient, "select province,num from tmp_city_num_detail", null, new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val tuple = (rs.getString(1), rs.getLong(2))
              history_data += tuple
            }
          }
        })
        val history_rdd = ssc.sparkContext.makeRDD(history_data)
        val resultRdd = history_rdd.fullOuterJoin(rdd).map(item => {
          val province = item._1
          val nums = item._2._1.getOrElse(0l) + item._2._2.getOrElse(0l)
          (province, nums)
        })
        resultRdd.foreachPartition(partitions => {
          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            partitions.foreach(item => {
              val province = item._1
              val num = item._2
              //修改mysql数据 并重组返回最新结果数据
              sqlProxy.executeUpdate(client, "insert into tmp_city_num_detail(province,num)values(?,?) on duplicate key update num=?",
                Array(province, num, num))
            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })
        val top3Rdd = resultRdd.sortBy[Long](_._2, false).take(3)
        sqlProxy.executeUpdate(ipClient, "truncate table top_city_num", null)
        top3Rdd.foreach(item => {
          sqlProxy.executeUpdate(ipClient, "insert into top_city_num (province,num) values(?,?)", Array(item._1, item._2))
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(ipClient)
      }
    })

    //计算转换率
    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {

      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        calcJumRate(sqlProxy, client) //计算转换率
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

  /**
    * 计算页面跳转个数
    *
    * @param sqlProxy
    * @param item
    * @param client
    */
  def calcPageJumpCount(sqlProxy: SqlProxy, item: (String, Int), client: Connection): Unit = {
    val keys = item._1.split("_")
    var num: Long = item._2
    val page_id = keys(1).toInt //获取当前page_id
    val last_page_id = keys(0).toInt //获取上一page_id
    val next_page_id = keys(2).toInt //获取下页面page_id
    //查询当前page_id的历史num个数
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(page_id), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          num += rs.getLong(1)
        }
        rs.close()
      }
    })
    //对num 进行修改 并且判断当前page_id是否为首页
    if (page_id == 1) {
      sqlProxy.executeUpdate(client, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num,jump_rate)" +
        "values(?,?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, num, "100%", num))
    } else {
      sqlProxy.executeUpdate(client, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num)" +
        "values(?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, num, num))
    }
  }

  /**
    * 计算转换率
    */
  def calcJumRate(sqlProxy: SqlProxy, client: Connection): Unit = {
    var page1_num = 0l
    var page2_num = 0l
    var page3_num = 0l
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(1), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page1_num = rs.getLong(1)
        }
      }
    })
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(2), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page2_num = rs.getLong(1)
        }
      }
    })
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(3), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page3_num = rs.getLong(1)
        }
      }
    })
    val nf = NumberFormat.getPercentInstance
    val page1ToPage2Rate = if (page1_num == 0) "0%" else nf.format(page2_num.toDouble / page1_num.toDouble)
    val page2ToPage3Rate = if (page2_num == 0) "0%" else nf.format(page3_num.toDouble / page2_num.toDouble)
    sqlProxy.executeUpdate(client, "update page_jump_rate set jump_rate=? where page_id=?", Array(page1ToPage2Rate, 2))
    sqlProxy.executeUpdate(client, "update page_jump_rate set jump_rate=? where page_id=?", Array(page2ToPage3Rate, 3))
  }

}
