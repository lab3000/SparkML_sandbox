package SparkMLExtension

import org.apache.spark.sql.SQLContext

/**
  * Created by lukmaanbawazer on 6/21/17.
  */
object ExploreCustoms {
  def main(args: Array[String]) {
    val sc = ContextConfig.main(args)
    // reuses code to create a SparkContext
    val sqlContext = new SQLContext(sc) // creates a SQLContext needed for DataFrames--be sure to import this // gives me the .toDF() method to turn an RDD into a DataFrame
    import sqlContext.implicits._ // gives me the .toDF() method to turn an RDD into a DataFrame

    val tweets = sc.textFile("./Tweets/testfile")

    val tweetsDF = tweets.toDF("value")

    val transformT = new CustomTransf()

    val x = transformT.transform(tweetsDF, true)

    val indexer = new SimpleIndexer()

    val model = indexer.fit(x)

    val predicted = model.transform(x)

    predicted.show()

  }
}
