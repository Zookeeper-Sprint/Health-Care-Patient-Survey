import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object query5 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Health Care Patient Survey Loader")
      .master("local[*]") // Use appropriate master setting for your environment
      .getOrCreate()

    val surveyDF: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .csv("C:/Users/dk25/Downloads/Health Care_Patient_survey_source.csv")


    val dfWithDate = surveyDF.withColumn("Measure End Date", to_date(col("Measure End Date"), "MM/dd/yyyy"))

    val topHospitals = surveyDF.groupBy("Hospital Name")
      .agg(avg("Survey Response Rate Percent").alias("Average Survey Rate"))
      .orderBy(desc("Average Survey Rate"))
      .na.drop()
      .limit(10)

    topHospitals.coalesce(1).write.option("header","true").mode("overwrite")
      .partitionBy("Hospital Name").orc("C:/Users/dk25/OneDrive - Capgemini/Desktop/HealthData_Opt/Query5")

    dfWithDate.show(truncate = false)

    // Stop SparkSession
    spark.stop()
  }
}
