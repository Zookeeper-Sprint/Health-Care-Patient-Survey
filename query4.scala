import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
object query4 {

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


    val dateCounts = surveyDF.groupBy("Measure Start Date")
      .count()
      .filter("count >= 1")  // Only dates with more than one survey

    // Step 3: Join back to get full details
    val resultDF = surveyDF.join(dateCounts, Seq("Measure Start Date"), "inner")

    // Step 4: Show the result
    resultDF.select("Hospital Name", "City", "State", "Measure ID", "Measure Start Date", "Answer Description")
      .orderBy("Measure Start Date")

    resultDF.coalesce(1).write.option("header","true").mode("overwrite")
      .partitionBy("City").csv("C:/Users/dk25/OneDrive - Capgemini/Desktop/HealthData_Opt/Query4")

    resultDF.show(truncate = false)


    // Stop SparkSession
    spark.stop()
  }
}
