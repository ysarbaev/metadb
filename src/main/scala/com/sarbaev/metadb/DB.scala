package com.sarbaev.metadb

import java.sql.{DriverManager, Driver, Connection}

/**
 * User: yuri
 * Date: 4/14/13
 * Time: 6:13 PM 
 */
case class DB(url: String, user: String, password: String) {

  def connect = {
    DriverManager.getConnection(url, user, password)
  }

  def withConnection[T](body: Connection => T): T = {
    val connection = connect
    try {
      body.apply(connection)
    } finally {
      connection.close()
    }
  }

}
