import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.io.StdIn
import scala.util.Try

object PCFullLoad extends App {

  val spark = SparkSession.builder()
    .appName("PCFullLoad")
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022")
    .config("spark.jars", "/usr/local/lib/postgresql-42.2.18.jar")
    .getOrCreate()

  // Function to load data from Postgres and add load timestamp
  def loadData(): Unit = {
    // Use the DatabaseConnection object to get JDBC URL and properties
    val jdbcUrl = DatabaseConnection.getJdbcUrl
    val jdbcProps = DatabaseConnection.getJdbcProperties

    // Query to fetch product_category data from Postgres
    val query = "(SELECT * FROM product_category) AS product_category"

    // Load data from Postgres
    val productCategoryDF = spark.read
      .option("header", "true")
      .jdbc(jdbcUrl, query, jdbcProps)

    productCategoryDF.show(20)

    // Add load timestamp
    val loadTimestamp = Timestamp.from(Instant.now())
    val productCategoryWithTimestampDF = productCategoryDF.withColumn("load_timestamp", lit(loadTimestamp))

    // For Demo 
    productCategoryWithTimestampDF.show(20)

    // Write the data to HDFS in Parquet format
    productCategoryWithTimestampDF.write
      .mode(SaveMode.Overwrite)
      .parquet("hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022/tmp/deep/stage1/full_load")
  }

  // Function to check if data has changed in Postgres or if new rows are added
  def hasDataChanged(): Boolean = {
    val jdbcUrl = DatabaseConnection.getJdbcUrl
    val jdbcProps = DatabaseConnection.getJdbcProperties

    // Fetch the latest last_modified_timestamp and row count from Postgres
    val query = "(SELECT MAX(last_modified_timestamp) as max_ts, COUNT(*) as total_records FROM product_category) AS latest"
    val latestStatsDF = spark.read
      .jdbc(jdbcUrl, query, jdbcProps)

    // Extract the max timestamp and row count
    val latestTimestamp = latestStatsDF.collect()(0).getAs[Timestamp]("max_ts")
    val latestRowCount = latestStatsDF.collect()(0).getAs[Long]("total_records")

    // Fetch the latest load timestamp and row count from HDFS (if it exists)
    val hdfsLoadPath = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022/tmp/deep/stage1/full_load"
    val lastLoadStats = Try {
      val hdfsDF = spark.read.parquet(hdfsLoadPath)
      val lastLoadTimestamp = hdfsDF.agg(max("load_timestamp")).collect()(0).getAs[Timestamp]("max(load_timestamp)")
      val lastLoadRowCount = hdfsDF.count()
      (Some(lastLoadTimestamp), lastLoadRowCount)
    }.getOrElse((None, 0L)) // If no previous load, set row count to 0

    val (lastLoadTimestamp, lastLoadRowCount) = lastLoadStats

    // Check if either the data has changed (timestamp) or new rows are added (row count)
    lastLoadTimestamp match {
      case Some(loadTimestamp) => latestTimestamp.after(loadTimestamp) || latestRowCount > lastLoadRowCount
      case None => true // If there's no previous load, consider it as changed
    }
  }

  // Function to prompt the user for input and decide whether to check for changes
  def promptUser(): Boolean = {
    println("Do you want to check for changes in PostgreSQL? (yes/no)")
    val input = StdIn.readLine().trim.toLowerCase
    input == "yes"
  }

  // Main loop that asks the user if they want to check for changes
  def startJob(): Unit = {
    var continue = true
    while (continue) {
      if (promptUser()) {
        println("Checking for data changes...")

        if (hasDataChanged()) {
          println("Data changed, performing full load...")
          loadData()
        } else {
          println("No changes detected.")
        }
      }

      // Optionally allow the user to exit the loop
      println("Do you want to continue checking for changes? (yes/no)")
      val continueInput = StdIn.readLine().trim.toLowerCase
      if (continueInput != "yes") continue = false
    }
  }

  // Start the job that asks the user for input
  startJob()

  // Stop the Spark session when the job ends
  spark.stop()
}
