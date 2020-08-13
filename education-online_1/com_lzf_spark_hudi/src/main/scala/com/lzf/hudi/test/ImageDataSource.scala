package com.lzf.hudi.test

import java.awt.image.{ColorModel, MemoryImageSource}
import java.io.{BufferedInputStream, ByteArrayInputStream, DataInput, DataInputStream, DataOutputStream, File, FileInputStream, FileOutputStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.sun.image.codec.jpeg.JPEGCodec
import javax.imageio.ImageIO
import javax.imageio.stream.FileImageOutputStream
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.opencv.core.{CvType, Mat, MatOfByte}
import org.opencv.imgcodecs.Imgcodecs

object ImageDataSource {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("dwd_member_import")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
//    readImageToHdfs(sparkSession)
    readImageFromHdfs(sparkSession)
  }

  def readImageToHdfs(sparkSession: SparkSession): Unit = {
    import org.apache.spark.sql.functions._
    val commitTime = System.currentTimeMillis().toString //生成提交时间
    val dt = DateTimeFormatter.ofPattern("yyyy/MM/dd").format(LocalDateTime.now())
    val df = sparkSession.read.format("image").option("dropInvalid", "true") //dropInvalid 是否删除无效图片
      .load("file://" + this.getClass.getResource("/").toURI.getPath)
    df.printSchema()
    val result = df.select("image.origin", "image.width", "image.height", "image.nChannels", "image.mode", "image.data")
      .withColumn("dt", lit(dt))
      .withColumn("ts", lit(commitTime))
      .withColumn("uuid", col("origin"))
    result.show()
    result.write.format("org.apache.hudi")
      .option("hoodie.insert.shuffle.parallelism", 12)
      .option("hoodie.upsert.shuffle.parallelism", 12)
      .option("PRECOMBINE_FIELD_OPT_KEY", "ts") //指定提交时间列
      .option("RECORDKEY_FIELD_OPT_KEY", "uuid") //指定唯一标示列
      .option("hoodie.table.name", "testTable")
      .option("hoodie.datasource.write.partitionpath.field", "dt") //分区列
      .mode(SaveMode.Overwrite)
      .save("/user/atguigu/hudi/image")
  }

  def readImageFromHdfs(sparkSession: SparkSession): Unit = {
    val df=sparkSession.read.format("org.apache.hudi")
      .load("/user/atguigu/hudi/image/*/*/*")
    df.printSchema()
    df.show()

    df.foreachPartition(partition=>{
      partition.foreach(row=>{
        val bytes=new Array[Byte](1024*1024)  //设置缓冲大小
        val data=row.getAs[Array[Byte]]("data")
        val hoodiefilename=row.getAs[String]("_hoodie_commit_seqno")
        val file = new File("D:\\tmp\\" + 1 + ".jpg") //写出路径
        if (!file.exists) file.createNewFile
        val fileImageOutputStream=new FileImageOutputStream(file)
        val width=row.getAs[Int]("width")
        val height=row.getAs[Int]("height")
        val imageProducer=new MemoryImageSource(width,height,ColorModel.getRGBdefault,data,0,width)
        imageProducer.newPixels()

//
//
      })
    })
  }
}
