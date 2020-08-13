package com.lzf.other.streaming

import java.io.DataOutputStream

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.mapred.lib.MultipleOutputFormat
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, RecordWriter, TextOutputFormat}
import org.apache.hadoop.util.{Progressable, ReflectionUtils}

class AppendTextOutputFormat extends TextOutputFormat[Any, Any] {

  override def getRecordWriter(ignored: FileSystem, job: JobConf, iname: String, progress: Progressable): RecordWriter[Any, Any] = {
    val isCompressed: Boolean = FileOutputFormat.getCompressOutput(job)
    val keyValueSeparator: String = job.get("mapreduce.output.textoutputformat.separator", "\t")
    //自定义输出文件名
    val name = job.get("filename",iname)
    if (!isCompressed) {
      val file: Path = FileOutputFormat.getTaskOutputPath(job, name)
      val fs: FileSystem = file.getFileSystem(job)
      val newFile : Path = new Path(FileOutputFormat.getOutputPath(job), name)
      val fileOut : FSDataOutputStream = if (fs.exists(newFile)) {
        //存在，追加写
        fs.append(newFile)
      } else {
        fs.create(file, progress)
      }
      new TextOutputFormat.LineRecordWriter[Any, Any](fileOut, keyValueSeparator)
    } else {
      val codecClass: Class[_ <: CompressionCodec] = FileOutputFormat.getOutputCompressorClass(job, classOf[GzipCodec])
      // create the named codec
      val codec: CompressionCodec = ReflectionUtils.newInstance(codecClass, job)
      // build the filename including the extension
      val file: Path = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension)
      val fs: FileSystem = file.getFileSystem(job)
      val newFile: Path = new Path(FileOutputFormat.getOutputPath(job), name + codec.getDefaultExtension)

      val fileOut: FSDataOutputStream = if (fs.exists(newFile)) {
        //存在，追加写
        fs.append(newFile)
      } else {
        fs.create(file, progress)
    }
      new TextOutputFormat.LineRecordWriter[Any, Any](new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator)
    }
  }
}


class RDDMultipleAppendTextOutputFormat extends MultipleOutputFormat[Any, Any]{
  private var theTextOutputFormat: AppendTextOutputFormat = null

  //产生分区目录
  //override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String ={
  //
  //  //TODO 分区目录
  //}

  //追加写
  override def
  getBaseRecordWriter(fs: FileSystem, job: JobConf, name: String, arg3: Progressable): RecordWriter[Any, Any] = {
    if (this.theTextOutputFormat == null) {
      this.theTextOutputFormat = new AppendTextOutputFormat()
    }
    this.theTextOutputFormat.getRecordWriter(fs, job, name, arg3)
  }
  //key重置为空
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()
}

