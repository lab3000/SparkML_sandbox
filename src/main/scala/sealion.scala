/*
/**
  * Created by lukmaanbawazer on 6/19/17.
  */
import org.apache.spark.{SparkConf, SparkContext}

object sealion {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val adding = sc.parallelize(1 to 1000).sum
    println("Success! Returned: " + adding)
  }



}
*/