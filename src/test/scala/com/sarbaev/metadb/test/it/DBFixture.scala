package com.sarbaev.metadb.test.it

import com.sarbaev.metadb.DB
import java.sql.Connection
import java.util.UUID
import com.sarbaev.metadb.postgresql.PGCatalog

/**
 * User: yuri
 * Date: 4/14/13
 * Time: 7:31 PM 
 */
trait DBFixture {

  Class.forName("org.postgresql.Driver")

  val db = DB("jdbc:postgresql://127.0.0.1/metadb", "postgres", "")

  implicit val connection = db.connect

  val schema = createSchema

  val schemaOid = PGCatalog.namespaces(Seq(schema)).head.oid

  def createSchema(implicit connection: Connection): String = {
    val sName = random
    exec(s"create schema $sName;")
    sName
  }

  def dropSchema = exec(s"drop schema $schema cascade;")

  def random: String = "random_" + UUID.randomUUID().toString.replaceAll("-", "_")

  def exec(sql: String*)(implicit connection: Connection): Unit = {
    sql.foreach {
      query =>
        val stmt = connection prepareStatement query
        stmt.execute
        stmt.close
    }
  }

  def cleanUp = {
    dropSchema
    connection.close
  }

}
