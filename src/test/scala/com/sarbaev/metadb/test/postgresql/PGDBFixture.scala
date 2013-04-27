package com.sarbaev.metadb.test.postgresql

import com.sarbaev.metadb.test.DBFixture
import com.sarbaev.metadb.DB
import java.sql.Connection

/**
 * User: yuri
 * Date: 4/27/13
 * Time: 11:24 PM 
 */
trait PGDBFixture extends DBFixture {
  override def driver: String = "org.postgresql.Driver"

  override def url: String = "jdbc:postgresql://127.0.0.1:5432/metadb"

  override def password: String = "postgres"

  override def user: String = ""

}
