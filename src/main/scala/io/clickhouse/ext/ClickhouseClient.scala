package io.clickhouse.ext

import ru.yandex.clickhouse.ClickHouseDataSource
import io.clickhouse.ext.Utils._

/**
  *
  *
  */
case class ClickhouseClient() (implicit ds : ClickHouseDataSource) {


	def query(sql : String) = {
		using(ds.getConnection) { conn =>
			val statement = conn.createStatement()
			val rs = statement.executeQuery(sql)
			rs
		}
	}

}