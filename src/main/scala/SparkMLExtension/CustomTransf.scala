package SparkMLExtension

/**
  * Created by lukmaanbawazer on 6/21/17.
  */
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.parsing.json.JSON

class CustomTransf(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("customtransf"))

  def copy(extra: ParamMap): CustomTransf = {
    defaultCopy(extra)
  }
  //end::basicPipelineSetup[]

  //tag::basicTransformSchema[]
  override def transformSchema(schema: StructType): StructType = {
  // Check that the input type is a string
  val idx = schema.fieldIndex("value")
  val field = schema.fields(idx)
  if (field.dataType != StringType) {
    throw new Exception(
      s"Input type ${field.dataType} did not match input type StringType")
  }
  // Add the return field
  schema.add(StructField("tweet", StringType, false)).add(StructField("lang", IntegerType, false))
}
  //end::basicTransformSchema[]

  //tag::transformFunction[]

  def transform(df: Dataset[_]): DataFrame = {
    val get_lang = udf {jsn: String => getTweetsAndLang(jsn)._2}
    val get_tweettext = udf {jsn: String => getTweetsAndLang(jsn)._1}
    df.select(col("*"),
      get_lang(df.col("value")).as("lang"),
      get_tweettext(df.col("value")).as("tweet")).where("tweet is not null").filter("lang > -1")

  }

  def transform(df: Dataset[_], only_en_es: Boolean): DataFrame = only_en_es match {

    case true => {
      val get_lang = udf {jsn: String => getTweetsAndLang(jsn)._2}
      val get_tweettext = udf {jsn: String => getTweetsAndLang(jsn)._1}
      df.select(col("*"),
        get_lang(df.col("value")).as("lang"),
        get_tweettext(df.col("value")).as("tweet")).where("tweet is not null").filter("lang > -1")
    }

    case false => {
      val get_lang = udf {jsn: String => findVal(jsn,"lang")}
      val get_tweettext = udf {jsn: String => findVal(jsn,"text")}
      df.select(col("*"),
        get_lang(df.col("value")).as("lang"),
        get_tweettext(df.col("value")).as("tweet")).where("text is not null")
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

///possible approach to custom estimator
class myEstimator(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("customtransf"))

  def copy(extra: ParamMap): CustomTransf = {
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
    val convert_lang = udf {in: String => if(in=="en") 0 else 1}
    df.select(col("*"),
      convert_lang(df.col("language")).as("lang_0_1"))

  }
}