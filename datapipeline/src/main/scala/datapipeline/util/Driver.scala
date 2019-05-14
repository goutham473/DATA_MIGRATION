package datapipeline.util
import datapipeline._
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON

case class TableProperties(tableNames: List[(String, String)])

class Driver {

  var dataPipeLineProps: DataPipeLineProperties = DataPipeLineProperties()

  def initializeDriver(args: Array[String])(sparkSession: SparkSession)  {
    loadTablePropertits(args(0))(sparkSession)
  }

  def loadTablePropertits(propConfigPath: String)(sparkSession: SparkSession) {
    val jsonMap = JSON.parseFull(sparkSession.read.textFile(propConfigPath).collect().mkString(" ")).get.asInstanceOf[Map[String, Any]]
    val jsonProps = jsonMap("dataPipeLineProperties").asInstanceOf[Map[String,List[Map[String, Map[String,Any]]]]]
    createTableInfo(jsonProps)
  }

  def createTableInfo(tableData: Map[String,List[Map[String, Map[String,Any]]]]) {
    val tableInfo = tableData("tablesInfo").flatMap{
      x => x.map(y => {
        var tables = Tables(y._1.asInstanceOf[String],y._2("backup_Table").asInstanceOf[String],y._2("history_Table").asInstanceOf[String])
        var keyColumns = KeyColumns(y._2("Key_Columns").asInstanceOf[List[Any]].map(_.toString), y._2("sequence_Id_Column").asInstanceOf[String])
        TableInfo(tables, keyColumns)
      })}
    val status_Table = tableData("status_table").asInstanceOf[String]
    dataPipeLineProps = dataPipeLineProps.copy(tableInfo, status_Table)
  }
}