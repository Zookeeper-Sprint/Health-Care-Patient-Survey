import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object query3 {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Health Care Patient Survey Loader")
      .master("local[*]") // Use appropriate master setting for your environment
      .getOrCreate()

    val surveyDF: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .csv("C:/Users/dk25/Downloads/Health Care_Patient_survey_source.csv")


    val topCounties = surveyDF
      .groupBy("County Name")
      .agg(avg("Survey Response Rate Percent").alias("Average Survey Rate"))
      .orderBy(desc("Average Survey Rate"))
      .na.drop() // Drop rows with nulls in County or Rate
      .limit(3)

      topCounties.coalesce(1).write.option("header","true").mode("overwrite")
      .partitionBy("County Name").csv("C:/Users/dk25/OneDrive - Capgemini/Desktop/HealthData_Opt/Query3")

    topCounties.show(truncate = false)

    // Stop SparkSession
    spark.stop()
  }
}
