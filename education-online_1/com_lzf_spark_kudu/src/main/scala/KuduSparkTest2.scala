import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//查询kudu
object KuduSparkTest2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val df = spark.read.options(Map("kudu.master" -> "hadoop001,hadoop002,hadoop003", "kudu.table" -> "test_table"))
      .format("org.apache.kudu.spark.kudu").load()
    df.show()
  }
}
