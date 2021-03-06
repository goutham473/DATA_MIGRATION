package datapipeline.model.write

import org.apache.spark.sql.{DataFrame, SparkSession}

object Write {

  def loadToHive(tableName: String, sequenceId: String)(df: DataFrame)(sparkSession: SparkSession) {
    val tempView = tableName.split(s"\\.")(1)
    df.sort(sequenceId).createOrReplaceTempView(s"$tempView")
    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sparkSession.sql(s"insert into table $tableName select * from $tempView")
  }
}
