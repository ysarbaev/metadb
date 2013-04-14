package com.sarbaev.metadb.test.it

import java.sql.Connection
import java.util.UUID
import com.sarbaev.metadb.DB
import org.scalatest.FreeSpec
import com.sarbaev.metadb.postgresql.PGCatalog
import org.scalatest.matchers.ShouldMatchers

/**
 * User: yuri
 * Date: 4/14/13
 * Time: 5:36 PM 
 */
class PGCatalogTests extends FreeSpec with ShouldMatchers {


  Class.forName("org.postgresql.Driver")

  val db = DB("jdbc:postgresql://127.0.0.1/metadb", "postgres", "")


  trait Fixture {

    implicit val connection = db.connect

    val schema = createSchema


    def createSchema(implicit connection: Connection): String = {
      val sName = random
      exec(s"create schema $sName;")
      sName
    }

    def dropSchema = exec(s"drop schema $schema cascade;")

    def random: String = "random_" + UUID.randomUUID().toString.replaceAll("-","_")

    def exec(sql: String*)(implicit connection: Connection): Unit = {
      sql.foreach { query =>
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

  "PGCatalog" - {

    "should create and drop a random schema" in new Fixture {

      val list = PGCatalog.namespaces(Seq(schema))

      list should have size(1)
      list.head.nspname should equal(schema)

    }.cleanUp

  }


}
