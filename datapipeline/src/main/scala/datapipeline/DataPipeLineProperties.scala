package datapipeline

case class DataPipeLineProperties(tables: List[TableInfo] = Nil, statusTable: String = " ") extends Serializable
