package datapipeline

case class KeyColumns(primaryKeyColumn: List[String], sequenceIdColumn: String)
case class Tables(baseTable: String, backUpTable: String, historyTable: String)
case class TableInfo(tableInfo: (Tables, KeyColumns))
