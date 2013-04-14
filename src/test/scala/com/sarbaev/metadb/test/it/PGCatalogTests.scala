package com.sarbaev.metadb.test.it

import java.sql.Connection
import java.util.UUID
import com.sarbaev.metadb.DB
import org.scalatest.FreeSpec
import com.sarbaev.metadb.postgresql.PGCatalog
import org.scalatest.matchers.ShouldMatchers
import com.sarbaev.metadb.postgresql.PGCatalog.PGType

/**
 * User: yuri
 * Date: 4/14/13
 * Time: 5:36 PM 
 */
class PGCatalogTests extends FreeSpec with ShouldMatchers {




  "PGCatalog" - {

    "should create and drop a random schema" in new DBFixture {

      val list = PGCatalog.namespaces(Seq(schema))

      list should have size(1)
      list.head.nspname should equal(schema)

    }.cleanUp

    "should retrieve types" in new DBFixture {

      exec(
        s"create table $schema.table_type_1(id int, str text)",
        s"create type $schema.enum_type_1 as enum ('a', 'b')"
      )


      val schemaId = PGCatalog.namespaces(Seq(schema)).head.oid

      val types = PGCatalog.types(Seq(schemaId))

      types should have size(4) //think about array types

      val fTypes = types.collect{
        case t@PGType(_, "table_type_1", _, _, _, 'c', 'C', _) => t
        case t@PGType(_, "enum_type_1", _, _, _, 'e', 'E', _) => t
      }

      fTypes should have size(2)

    }

  }


}
