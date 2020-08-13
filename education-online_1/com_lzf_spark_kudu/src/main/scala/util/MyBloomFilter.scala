//package util
//
//import com.google.common.hash.{BloomFilter, Funnels}
//
//object MyBloomFilter {
//
//  def main(args: Array[String]): Unit = {
//    val size = 1000000 //预计要插入多少条数据
//    val fpp = 0.01 //期望的误判率
//    val bloomFilter = BloomFilter.create(Funnels.integerFunnel(), size, fpp)
//    for (i <- 0 until 1000000) {
//      bloomFilter.put(i)
//    }
//    for (i <- 0 until 2000000) {
//      if (bloomFilter.mightContain(i)) {
//        println(i + "存在")
//      }
//    }
//  }
//}
