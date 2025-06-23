import org.apache.spark.sql.{SparkSession, DataFrame}

import org.apache.spark.sql.functions._

object query2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Health Care Patient Survey Loader")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")


    val surveyDF: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .option("quote", "\"")
      .option("nullValue", "Not Applicable")
      .csv("C:/Users/dk25/Downloads/Health Care_Patient_survey_source.csv")


    // Calculate average response rate by Measure ID

    val responseRateByMeasure = surveyDF
      .groupBy("Measure ID")
      .agg(avg("Survey Response Rate Percent").alias("Average Response Rate"))

    responseRateByMeasure.coalesce(1).write.option("header","true").mode("overwrite")
          .partitionBy("Measure ID").csv("C:/Users/dk25/OneDrive - Capgemini/Desktop/HealthData_Opt/query2")


    spark.stop()

  }

}