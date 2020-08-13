import org.lionsoul.ip2region.{DbConfig, DbSearcher}

object IpTest {
  def main(args: Array[String]): Unit = {
    val ipSearch = new DbSearcher(new DbConfig(), this.getClass.getResource("/ip2region.db").getPath)
    val region = ipSearch.binarySearch("121.77.192.102").getRegion
    println(region)
    //    val city = region.split("\\|")(2)
    //    println(city)
  }
}
