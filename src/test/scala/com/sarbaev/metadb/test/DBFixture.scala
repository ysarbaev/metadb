package com.sarbaev.metadb.test

import com.sarbaev.metadb.DB
import java.sql.Connection
/**
 * User: yuri
 * Date: 4/27/13
 * Time: 10:54 PM 
 */
trait DBFixture extends AutoCloseable{

  def driver: String

  def url: String

  def user: String

  def password: String

  val driverClass = Class.forName(driver)

  val db = DB(url, user, password)

  implicit val connection = db.connect

  def exec(sql: String*): Unit = {
    sql.foreach {
      query =>
        val stmt = connection prepareStatement query
        stmt.execute
        stmt.close
    }
  }

  override def close() {
    connection.close
  }

  def closeAfter(f: => Unit) {
    try{
      f
    } finally {
      close
    }
  }
}
