package SparkMLExtension

/**
  * Created by lukmaanbawazer on 6/19/17.
  */

import org.apache.spark.{SparkConf, SparkContext}

object ContextConfig {

  def main(args: Array[String]): SparkContext = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]").set( "spark.driver.host", "localhost" )
    val sc: SparkContext = new SparkContext(conf)
//    val adding = sc.parallelize(1 to 1000).sum
//    println("Success! Returned: " + adding)
    sc
}

}
