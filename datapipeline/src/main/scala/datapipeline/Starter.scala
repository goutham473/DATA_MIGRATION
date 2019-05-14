package datapipeline

import datapipeline.util.Driver
import org.apache.spark.sql.SparkSession

import datapipeline.model.process.Process

import scala.util.{Try, Success, Failure}

object Starter extends App{

  val hiveMetaStore = args(1)
  val propsPath = args(0)
  val sparkSession = createSparkSession(hiveMetaStore)
  val driver: Driver = initializeDriver(propsPath)(sparkSession)


  Try{
    Process.invokeDataLoading(driver)(sparkSession)
  } match {
    case Success(_) => sparkSession.stop()
    case Failure(e) =>  sparkSession.stop(); throw e
  }

  def createSparkSession(hiveMetaStore: String): SparkSession = {
    SparkSession
      .builder()
      .config("hive.metastore.uris", hiveMetaStore)
      .appName("Data_Pipe_Line")
      .enableHiveSupport()
      .getOrCreate()
  }

  def initializeDriver(path: String)(sparkSession: SparkSession): Driver = {
    val driver = new Driver
    driver.loadTablePropertits(path)(sparkSession)
    driver
  }
}
