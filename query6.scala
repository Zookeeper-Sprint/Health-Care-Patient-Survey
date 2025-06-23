import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
object query6 {

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

    val cleanedDF = surveyDF.na.drop(Seq("State", "City", "Survey Response Rate Percent"))
      .withColumn("Survey Response Rate Percent", col("Survey Response Rate Percent").cast("double"))

    val drillDownReport = cleanedDF
      .groupBy("State", "City")
      .agg(avg("Survey Response Rate Percent").alias("Average Survey Rate"))
      .orderBy("State", "City")

      drillDownReport.coalesce(1).write.option("header","true").mode("overwrite")
      .partitionBy("State").csv("C:/Users/dk25/OneDrive - Capgemini/Desktop/HealthData_Opt/Query6")

    drillDownReport.show(50, truncate = false)


    spark.stop()
  }
}
