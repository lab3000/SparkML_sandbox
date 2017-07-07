package SparkMLExtension

/**
  * Created by lukmaanbawazer on 6/19/17.
  */

import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.parsing.json.JSON


class HardCodedWordCountStage(override val uid: String) extends Transformer {
//  def transform(tweets: RDD[String]) = ???

  def this() = this(Identifiable.randomUID("hardcodedwordcount"))

  def copy(extra: ParamMap): HardCodedWordCountStage = {
    defaultCopy(extra)
  }
  //end::basicPipelineSetup[]

  //tag::basicTransformSchema[]
  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex("happy_pandas")
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(
        s"Input type ${field.dataType} did not match input type StringType")
    }
    // Add the return field
    schema.add(StructField("happy_panda_counts", IntegerType, false))
  }
  //end::basicTransformSchema[]

  //tag::transformFunction[]

  def transform(df: Dataset[_]): DataFrame = {
      val get_lang = udf {jsn: String => getTweetsAndLang(jsn)._2}
      val get_tweettext = udf {jsn: String => getTweetsAndLang(jsn)._1}
      df.select(col("*"),
        get_lang(df.col("json_tweets")).as("language"),
        get_tweettext(df.col("json_tweets")).as("text")).where("text is not null").filter("language > -1")

  }

  def transform(df: Dataset[_], only_en_es: Boolean): DataFrame = only_en_es match {

    case true => {
      val get_lang = udf {jsn: String => getTweetsAndLang(jsn)._2}
      val get_tweettext = udf {jsn: String => getTweetsAndLang(jsn)._1}
      df.select(col("*"),
        get_lang(df.col("json_tweets")).as("language"),
        get_tweettext(df.col("json_tweets")).as("text")).where("text is not null").filter("language > -1")
    }

    case false => {
      val get_lang = udf {jsn: String => findVal(jsn,"lang")}
      val get_tweettext = udf {jsn: String => findVal(jsn,"text")}
      df.select(col("*"),
        get_lang(df.col("json_tweets")).as("language"),
        get_tweettext(df.col("json_tweets")).as("text")).where("text is not null")
    }
  }
  /*
  def transform(df: Dataset[_], only_en_es: Boolean): DataFrame = {

    if(only_en_es){
      val get_lang = udf {jsn: String => getTweetsAndLang(jsn)._2}
      val get_tweettext = udf {jsn: String => getTweetsAndLang(jsn)._1}
      df.select(col("*"),
      get_lang(df.col("json_tweets")).as("language"),
      get_tweettext(df.col("json_tweets")).as("text")).where("text is not null").filter("language > -1")
    }

    else{
      val get_lang = udf {jsn: String => findVal(jsn,"lang")}
      val get_tweettext = udf {jsn: String => findVal(jsn,"text")}
      df.select(col("*"),
        get_lang(df.col("json_tweets")).as("language"),
        get_tweettext(df.col("json_tweets")).as("text")).where("text is not null")
    }
  }
*/

  /*
  def transform(df: Dataset[_]): DataFrame = {
    val get_lang = udf {jsn: String => getTweetsAndLang(jsn)._2}
    val get_tweettext = udf {jsn: String => getTweetsAndLang(jsn)._1}
    df.select(col("*"),
      get_lang(df.col("json_tweets")).as("language"),
      get_tweettext(df.col("json_tweets")).as("text")).where("text is not null").where("language is not other")
  }
  //end::transformFunction[]
*/



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


  def findVal(str: String, ToFind: String): String = {
    try {
      JSON.parseFull(str) match {
        case Some(m: Map[String, String]) => m(ToFind)
      }
    } catch {
      case e: Exception => null
    }
  }

}