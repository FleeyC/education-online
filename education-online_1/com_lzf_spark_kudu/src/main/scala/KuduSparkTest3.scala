import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object KuduSparkTest3 {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkContext = spark.sparkContext
    val kuduContext = new KuduContext("hadoop001,hadoop002,hadoop003", sparkContext)
    val testTableDF = spark.read.options(Map("kudu.master" -> "hadoop001,hadoop002,hadoop003", "kudu.table" -> "test_table"))
      .format("org.apache.kudu.spark.kudu").load()
    val flag = kuduContext.tableExists("test_table") //判断表是否存在
    if (flag) {
      import spark.implicits._
      val tuple = (1, "张三")
      val df = sparkContext.makeRDD(Seq(tuple)).toDF("id","name")
      kuduContext.insertRows(df, "test_table") //往test_table表中插入 id为1  name为张三的数据
      testTableDF.show()
      val tuple2 = (1, "李四")
      val df2 = sparkContext.makeRDD(Seq(tuple2)).toDF("id","name")
      kuduContext.updateRows(df2, "test_table") //将test_table表中主键id为1 的数据name值修改为李四
      testTableDF.show()
      kuduContext.deleteRows(df2, "test_table") //将test_table表中的主键id为1 name值为李四的数据删除
      testTableDF.show()
    }
  }
}
