import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
object query1 {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Health Care Patient Survey Loader")
      .master("local[*]") // Use appropriate master setting for your environment
      .getOrCreate()


    // Load the CSV file into a DataFrame
    val surveyDF: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .csv("C:/Users/dk25/Downloads/Health Care_Patient_survey_source.csv")

    // Display schema and sample data

    val surveyCountDF = surveyDF
      .groupBy("Hospital Name") // Replace with actual column name if different
      .agg(count("*").alias("Survey Count"))
      .orderBy(desc("Survey Count"))

      surveyCountDF.coalesce(1).write.mode("overwrite").parquet("C:/Users/dk25/OneDrive - Capgemini/Desktop/HealthData_Opt/SurveyCountDF")
    //surveyCountDF.show()

    // Stop SparkSession
    spark.stop()
  }
}