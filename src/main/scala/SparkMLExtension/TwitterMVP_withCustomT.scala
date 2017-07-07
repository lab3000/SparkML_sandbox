package SparkMLExtension


/**
  * Created by lukmaanbawazer on 6/19/17.
  */
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator}
import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import scala.util.parsing.json._


object TwitterMVP_withCustomT {
  def main(args: Array[String]) {
    val sc = SparkMLExtension.ContextConfig.main(args)
    // reuses code to create a SparkContext
    val sqlContext = new SQLContext(sc) // creates a SQLContext needed for DataFrames--be sure to import this
    import sqlContext.implicits._ // gives me the .toDF() method to turn an RDD into a DataFrame

    val tweets = sc.textFile("./Tweets/testfile").toDF("value")

    val transformT = new CustomTransf()

    val tweetlangDF = transformT.transform(tweets, true)
    //val tweetlangDF = tweets.map(getTweetsAndLang).filter(x => x._2 != -1).toDF("tweet","lang")

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("tweet").setOutputCol("words").setPattern("\\s+|[,.\"]")

    //val tweetlangDF_tokenized = regexTokenizer.transform(tweetlangDF)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(200)

    //val tweetlangDF_hashed = hashingTF.transform(tweetlangDF_tokenized)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    val forestizer = new RandomForestClassifier().setLabelCol("lang").setFeaturesCol("features").setNumTrees(10)

    val pipeline = new Pipeline().setStages(Array(regexTokenizer,hashingTF,idf,forestizer))

    val Array(train_data,test_data) = tweetlangDF.randomSplit(Array(0.7,0.3))

    val tweet_estimator = pipeline.fit(train_data)

    val tweet_preds = tweet_estimator.transform(test_data)

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("lang")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")
    val auc = evaluator.evaluate(tweet_preds)

    println("auc = " +auc)

  }

  def findVal(str: String, ToFind: String): String = {
    try {
      JSON.parseFull(str) match {
        case Some(m: Map[String, String]) => m(ToFind)
      }
    } catch {
      case e: Exception => null
    }
  }


  def getTweetsAndLang(input: String): (String, Int) = {
    try {
      var result = (findVal(input, "text"), -1)

      if (findVal(input, "lang") == "en") result.copy(_2 = 0)
      else if (findVal(input, "lang") == "es") result.copy(_2 = 1)
      else result
    } catch {
      case e: Exception => ("unknown", -1)
    }
  }
}
