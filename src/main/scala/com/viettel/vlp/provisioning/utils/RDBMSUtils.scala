package com.viettel.vlp.provisioning.utils

import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.collection.mutable.ListBuffer

class RDBMSUtils {
  //insert data to RDBMS
  def insertDataToRDBMS(df: DataFrame, mapRdbmsConfig: Map[String, List[String]]): Unit = {
    //  config connect PostgreSQL
    val jdbcUrl = mapRdbmsConfig(Const.JDBC_URL_CONFIG).head
    val dbTable = mapRdbmsConfig(Const.TABLE_NAME).head
    val jdbcProperties = new java.util.Properties()
    jdbcProperties.setProperty("user", mapRdbmsConfig(Const.USERNAME).head)
    jdbcProperties.setProperty("password", mapRdbmsConfig(Const.PASSWORD).head)
    jdbcProperties.setProperty("driver", mapRdbmsConfig(Const.DRIVER).head)
    df.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, dbTable, jdbcProperties)
  }

  // execute query
  def executeQueryDB(mapRdbmsConfig: Map[String, List[String]], sql: String): Unit = {


    // load driver
    Class.forName(mapRdbmsConfig(Const.DRIVER).head)

    // connect db
    var connection: Connection = null
    var resultSet: ResultSet = null

    try {
      connection = DriverManager.getConnection(mapRdbmsConfig(Const.JDBC_URL_CONFIG).head, mapRdbmsConfig(Const.USERNAME).head, mapRdbmsConfig(Const.PASSWORD).head)
      val statement = connection.createStatement()
      resultSet = statement.executeQuery(sql)


      val results = ListBuffer[(String, String, String)]()
      while (resultSet.next()) {
        val id = resultSet.getString("uuid")
        val name = resultSet.getString("full_name")
        val age = resultSet.getString("identification_card")
        results += ((id, name, age))
      }


      results.foreach {
        case (id, name, age) =>
          println(s"ID: $id, Name: $name, IdentificationCard: $age")
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {

      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }
  }
}
