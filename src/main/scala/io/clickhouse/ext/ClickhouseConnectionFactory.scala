package io.clickhouse.ext

import java.util.Properties

import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties

object ClickhouseConnectionFactory extends Serializable {


	private val pool = scala.collection.mutable.Map[(String, Int, String), ClickHouseDataSource]()

		def get(host : String, port : Int = 8123, dbName : String) : ClickHouseDataSource = {
			pool.get((host, port, dbName)) match {


			case Some(ds) =>
				ds
			case None =>
				val ds = createDatasource(host, port = port, dbName)
				pool += ((host, port, dbName) -> ds)
				ds
		}
	}

	private def createDatasource(host : String, port : Int = 8123, dbName : String) = {

		val props = new ClickHouseProperties()

		props.setSocketTimeout(180000) // 30000 is default value
		props.setDataTransferTimeout(60000) // 10000 is default value

		val clickHouseProps = new ClickHouseProperties(props)
		new ClickHouseDataSource(s"jdbc:clickhouse://$host:$port/$dbName", clickHouseProps)
	}
}
