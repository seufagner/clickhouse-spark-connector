package io.clickhouse.ext.spark

import java.util.Calendar

import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import io.clickhouse.ext.ClickhouseClient
import io.clickhouse.ext.ClickhouseConnectionFactory
import io.clickhouse.ext.Utils.using
import ru.yandex.clickhouse.ClickHouseDataSource
import org.apache.spark.sql.types.ArrayType

/**
  * Extends Spark DataFrame using scala implicits
  */
object ClickhouseSparkExt {
  implicit def extraOperations(df: org.apache.spark.sql.DataFrame) = DataFrameExt(df)
}

case class DataFrameExt(df: org.apache.spark.sql.DataFrame) extends Serializable {

  /**
    * Creates a new table if it not exists
    *
    * @param dbName
    * @param tableName
    * @param partitionColumnName [ Date field ] that will be used MergeTree engine splits data into partitions by months,
    *                            and the engine needs a way to know what date a record belongs to and it only was taught to use Date typed field for the purpose.
    * @param indexColumns        ??
    * @param clusterName         Specify cluster name if you want to create Distributed table
    *
    */
  def createClickhouseTable(dbName: String, tableName: String, partitionColumnName: String, indexColumns: Seq[String])(implicit ds: ClickHouseDataSource) {
    assert(dbName != "")
    assert(tableName != "")
    assert(partitionColumnName != "")

    val client = ClickhouseClient()(ds)
    val sqlStmt = createClickhouseTableDefinitionSQL(dbName, tableName, partitionColumnName, indexColumns)

    client.query(sqlStmt)
  }

  /**
    * Drop existing table.
    * It doesn’t return an error if the table doesn’t exist or the database doesn’t exist.
    *
    * @param dbName
    * @param tableName
    * @param clusterName Specify cluster name if you want to delete table on entire cluster
    *
    */
  def dropClickhouseTable(dbName: String, tableName: String)(implicit ds: ClickHouseDataSource) {
    assert(dbName != "")
    assert(tableName != "")

    val client = ClickhouseClient()(ds)
    var sqlStmt = s"DROP TABLE IF EXISTS $dbName.$tableName"


    client.query(sqlStmt)
  }

  def renameClickhouseTable(dbName: String, tableName: String, newTable: String)(implicit ds: ClickHouseDataSource) {
    assert(dbName != "")
    assert(tableName != "")

    val client = ClickhouseClient()(ds)
    var sqlStmt = s"RENAME TABLE $dbName.$tableName TO $dbName.$newTable"


    client.query(sqlStmt)
  }

  /**
    * Save data on specific table
    *
    * @param partitionFunc       function that will be calculate new Date column partition
    * @param partitionColumnName Name of Date column partition
    * @param batchSize           Number of simultaneous inserts
    *
    */
  def saveToClickhouse(dbName: String, tableName: String, partitionFunc: (org.apache.spark.sql.Row) => java.sql.Date,
                       partitionColumnName: String = "partition_column_date", batchSize: Int = 100000)(implicit ds: ClickHouseDataSource) = {

    val targetHost = ds.getHost
    val defaultPort = ds.getPort

    val schema = df.schema

    // following code is going to be run on executors
    val insertResults = df.rdd.mapPartitions((partition: Iterator[org.apache.spark.sql.Row]) => {

      val targetHostDs = ClickhouseConnectionFactory.get(targetHost, defaultPort, dbName)

      // explicit closing
      using(targetHostDs.getConnection) { conn =>

        val insertStatementSql = generateInsertStatment(schema, dbName, tableName, partitionColumnName)
        val statement = conn.prepareStatement(insertStatementSql)

        var totalInsert = 0
        var counter = 0

        while (partition.hasNext) {

          counter += 1
          val row = partition.next()

          val partitionVal = partitionFunc(row)
          statement.setDate(1, partitionVal)

          // map fields
          schema.foreach { f =>
            val fieldName = f.name
            val fieldIdx = row.fieldIndex(fieldName)
            val fieldVal = row.get(fieldIdx)
            if (fieldVal != null) {
              statement.setObject(fieldIdx + 2, fieldVal)
            } else {
              val defVal = defaultNullValue(f.dataType, fieldVal)
              statement.setObject(fieldIdx + 2, defVal)
            }
          }
          statement.addBatch()

          if (counter >= batchSize) {
            val r = statement.executeBatch()
            totalInsert += r.sum
            counter = 0
          }

        } // end: while

        if (counter > 0) {
          val r = statement.executeBatch()
          totalInsert += r.sum
          counter = 0
        }

        List((targetHost, totalInsert)).toIterator
      }

    }).collect()

  }

  private def generateInsertStatment(schema: org.apache.spark.sql.types.StructType, dbName: String, tableName: String, partitionColumnName: String) = {
    val columns = partitionColumnName :: schema.map(f => f.name).toList
    val vals = 1 to (columns.length) map (i => "?")
    s"INSERT INTO $dbName.$tableName (${columns.mkString(",")}) VALUES (${vals.mkString(",")})"
  }

  private def defaultNullValue(sparkType: org.apache.spark.sql.types.DataType, v: Any) = sparkType match {
    case DoubleType => 0
    case LongType => 0
    case FloatType => 0
    case IntegerType => 0
    case StringType => null
    case BooleanType => false
    case DateType => java.sql.Timestamp.valueOf("1970-01-01 12:00:00")
    case TimestampType => new java.sql.Timestamp(System.currentTimeMillis())
    case _ => null
  }

  private def createClickhouseTableDefinitionSQL(dbName: String, tableName: String, partitionColumnName: String = "partition_date", indexColumns: Seq[String]) = {

    val header =
      s"""
          CREATE TABLE IF NOT EXISTS $dbName.$tableName(
          """

    val columns = s"$partitionColumnName Date" :: df.schema.map { f =>
      Seq(f.name, sparkType2ClickhouseType(f.dataType)).mkString(" ")
    }.toList

    val columnsStr = columns.mkString(",\n")

    val footer =
      s"""
          )ENGINE = MergeTree($partitionColumnName, (${indexColumns.mkString(",")}), 8192);
          """

    Seq(header, columnsStr, footer).mkString("\n")
  }

  private def sparkType2ClickhouseType(sparkType: org.apache.spark.sql.types.DataType) = sparkType match {
    case LongType => "Int64"
    case DoubleType => "Float64"
    case FloatType => "Float32"
    case IntegerType => "Int32"
    case StringType => "String"
    case BooleanType => "UInt8"
    case DateType => "Date"
    case TimestampType => "DateTime"
    case _ => "unknown"
  }

}
