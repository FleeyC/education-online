package com.lzf.qzpoint.streaming

import java.lang
import java.sql.{Connection, ResultSet}

import com.lzf.qzpoint.bean.LearnModel
import com.lzf.qzpoint.util.{DataSourceUtil, ParseJsonData, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CourseLearnStreaming {
  private val groupid = "course_learn_test1"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val topics = Array("course_learn")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //查询mysql是否存在偏移量
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
    val dsStream = stream.mapPartitions(partitions => {
      partitions.map(item => {
        val json = item.value()
        val jsonObject = ParseJsonData.getJsonData(json)
        val userId = jsonObject.getIntValue("uid")
        val cwareid = jsonObject.getIntValue("cwareid")
        val videoId = jsonObject.getIntValue("videoid")
        val chapterId = jsonObject.getIntValue("chapterid")
        val edutypeId = jsonObject.getIntValue("edutypeid")
        val subjectId = jsonObject.getIntValue("subjectid")
        val sourceType = jsonObject.getString("sourceType") //播放设备来源
        val speed = jsonObject.getIntValue("speed") //速度
        val ts = jsonObject.getLong("ts") //开始时间
        val te = jsonObject.getLong("te") //结束时间
        val ps = jsonObject.getIntValue("ps") //视频开始区间
        val pe = jsonObject.getIntValue("pe") //视频结束区间
        LearnModel(userId, cwareid, videoId, chapterId, edutypeId, subjectId, sourceType, speed, ts, te, ps, pe)
      })
    })
    dsStream.foreachRDD(rdd => {
      rdd.cache()
      //统计播放视频 有效时长 完成时长 总时长
      rdd.groupBy(item => item.userId + "_" + item.cwareId + "_" + item.videoId).foreachPartition(partitoins => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitoins.foreach { case (key, iters) =>
            calcVideoTime(key, iters, sqlProxy, client) //计算视频时长
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
      //统计章节下视频播放总时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.chapterId
          (key, totaltime)
        })
      }).reduceByKey(_ + _)
        .foreachPartition(partitoins => {
          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            partitoins.foreach(item => {
              sqlProxy.executeUpdate(client, "insert into chapter_learn_detail(chapterid,totaltime) values(?,?) on duplicate key" +
                " update totaltime=totaltime+?", Array(item._1, item._2, item._2))
            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })

      //统计课件下的总播放时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.cwareId
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitions => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach(item => {
            sqlProxy.executeUpdate(client, "insert into cwareid_learn_detail(cwareid,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })

      //统计辅导下的总播放时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.edutypeId
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitions => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach(item => {
            sqlProxy.executeUpdate(client, "insert into edutype_learn_detail(edutypeid,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })

      //统计同一资源平台下的总播放时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.sourceType
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitions => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach(item => {
            sqlProxy.executeUpdate(client, "insert into sourcetype_learn_detail (sourcetype_learn,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
      // 统计同一科目下的播放总时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.subjectId
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitons => {
        val sqlProxy = new SqlProxy()
        val clinet = DataSourceUtil.getConnection
        try {
          partitons.foreach(item => {
            sqlProxy.executeUpdate(clinet, "insert into subject_learn_detail(subjectid,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(clinet)
        }
      })

    })
    //计算转换率
    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
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

  /**
    * 计算视频 有效时长  完成时长 总时长
    *
    * @param key
    * @param iters
    * @param sqlProxy
    * @param client
    */
  def calcVideoTime(key: String, iters: Iterable[LearnModel], sqlProxy: SqlProxy, client: Connection) = {
    val keys = key.split("_")
    val userId = keys(0).toInt
    val cwareId = keys(1).toInt
    val videoId = keys(2).toInt
    //查询历史数据
    var interval_history = ""
    sqlProxy.executeQuery(client, "select play_interval from video_interval where userid=? and cwareid=? and videoid=?",
      Array(userId, cwareId, videoId), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            interval_history = rs.getString(1)
          }
          rs.close()
        }
      })
    var effective_duration_sum = 0l //有效总时长
    var complete_duration_sum = 0l //完成总时长
    var cumulative_duration_sum = 0l //播放总时长
    val learnList = iters.toList.sortBy(item => item.ps) //转成list 并根据开始区间升序排序
    learnList.foreach(item => {
      if ("".equals(interval_history)) {
        //没有历史区间
        val play_interval = item.ps + "-" + item.pe //有效区间
        val effective_duration = Math.ceil((item.te - item.ts) / 1000) //有效时长
        val complete_duration = item.pe - item.ps //完成时长
        effective_duration_sum += effective_duration.toLong
        cumulative_duration_sum += effective_duration.toLong
        complete_duration_sum += complete_duration
        interval_history = play_interval
      } else {
        //有历史区间进行对比
        val interval_arry = interval_history.split(",").sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt))
        val tuple = getEffectiveInterval(interval_arry, item.ps, item.pe)
        val complete_duration = tuple._1 //获取实际有效完成时长
        val effective_duration = Math.ceil((item.te - item.ts) / 1000) / (item.pe - item.ps) * complete_duration //计算有效时长
        val cumulative_duration = Math.ceil((item.te - item.ts) / 1000) //累计时长
        interval_history = tuple._2
        effective_duration_sum += effective_duration.toLong
        complete_duration_sum += complete_duration
        cumulative_duration_sum += cumulative_duration.toLong
      }
      sqlProxy.executeUpdate(client, "insert into video_interval(userid,cwareid,videoid,play_interval) values(?,?,?,?) " +
        "on duplicate key update play_interval=?", Array(userId, cwareId, videoId, interval_history, interval_history))
      sqlProxy.executeUpdate(client, "insert into video_learn_detail(userid,cwareid,videoid,totaltime,effecttime,completetime) " +
        "values(?,?,?,?,?,?) on duplicate key update totaltime=totaltime+?,effecttime=effecttime+?,completetime=completetime+?",
        Array(userId, cwareId, videoId, cumulative_duration_sum, effective_duration_sum, complete_duration_sum, cumulative_duration_sum,
          effective_duration_sum, complete_duration_sum))
    })
  }

  /**
    * 计算有效区间
    *
    * @param array
    * @param start
    * @param end
    * @return
    */
  def getEffectiveInterval(array: Array[String], start: Int, end: Int) = {
    var effective_duration = end - start
    var bl = false //是否对有效时间进行修改
    import scala.util.control.Breaks._
    breakable {
      for (i <- 0 until array.length) {
        //循环各区间段
        var historyStart = 0 //获取其中一段的开始播放区间
        var historyEnd = 0 //获取其中一段结束播放区间
        val item = array(i)
        try {
          historyStart = item.split("-")(0).toInt
          historyEnd = item.split("-")(1).toInt
        } catch {
          case e: Exception => throw new Exception("error array:" + array.mkString(","))
        }
        if (start >= historyStart && historyEnd >= end) {
          //已有数据占用全部播放时长 此次播放无效
          effective_duration = 0
          bl = true
          break()
        } else if (start <= historyStart && end > historyStart && end < historyEnd) {
          //和已有数据左侧存在交集 扣除部分有效时间（以老数据为主进行对照）
          effective_duration -= end - historyStart
          array(i) = start + "-" + historyEnd
          bl = true
        } else if (start > historyStart && start < historyEnd && end >= historyEnd) {
          //和已有数据右侧存在交集 扣除部分有效时间
          effective_duration -= historyEnd - start
          array(i) = historyStart + "-" + end
          bl = true
        } else if (start < historyStart && end > historyEnd) {
          //现数据 大于旧数据 扣除旧数据所有有效时间
          effective_duration -= historyEnd - historyStart
          array(i) = start + "-" + end
          bl = true
        }
      }
    }
    val result = bl match {
      case false => {
        //没有修改原array 进行新增没有交集
        val distinctArray2 = ArrayBuffer[String]()
        distinctArray2.appendAll(array)
        distinctArray2.append(start + "-" + end)
        val distinctArray = distinctArray2.distinct.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt))
        val tmpArray = ArrayBuffer[String]()
        tmpArray.append(distinctArray(0))
        for (i <- 1 until distinctArray.length) {
          val item = distinctArray(i).split("-")
          val tmpItem = tmpArray(tmpArray.length - 1).split("-")
          val itemStart = item(0)
          val itemEnd = item(1)
          val tmpItemStart = tmpItem(0)
          val tmpItemEnd = tmpItem(1)
          if (tmpItemStart.toInt < itemStart.toInt && tmpItemEnd.toInt < itemStart.toInt) {
            //没有交集
            tmpArray.append(itemStart + "-" + itemEnd)
          } else {
            //有交集
            val resultStart = tmpItemStart
            val resultEnd = if (tmpItemEnd.toInt > itemEnd.toInt) tmpItemEnd else itemEnd
            tmpArray(tmpArray.length - 1) = resultStart + "-" + resultEnd
          }
        }
        val play_interval = tmpArray.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt)).mkString(",")
        play_interval
      }
      case true => {
        //修改了原array 进行区间重组
        val distinctArray = array.distinct.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt))
        val tmpArray = ArrayBuffer[String]()
        tmpArray.append(distinctArray(0))
        for (i <- 1 until distinctArray.length) {
          val item = distinctArray(i).split("-")
          val tmpItem = tmpArray(tmpArray.length - 1).split("-")
          val itemStart = item(0)
          val itemEnd = item(1)
          val tmpItemStart = tmpItem(0)
          val tmpItemEnd = tmpItem(1)
          if (tmpItemStart.toInt < itemStart.toInt && tmpItemEnd.toInt < itemStart.toInt) {
            //没有交集
            tmpArray.append(itemStart + "-" + itemEnd)
          } else {
            //有交集
            val resultStart = tmpItemStart
            val resultEnd = if (tmpItemEnd.toInt > itemEnd.toInt) tmpItemEnd else itemEnd
            tmpArray(tmpArray.length - 1) = resultStart + "-" + resultEnd
          }
        }
        val play_interval = tmpArray.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt)).mkString(",")
        play_interval
      }
    }
    (effective_duration, result)
  }
}
