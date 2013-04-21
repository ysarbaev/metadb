package com.sarbaev.metadb.test

import java.sql.Connection
import java.util.UUID
import com.sarbaev.metadb.DB
import org.scalatest.{matchers, FreeSpec}
import com.sarbaev.metadb.postgresql.PGCatalog
import org.scalatest.matchers.ShouldMatchers
import com.sarbaev.metadb.postgresql.PGCatalog.{PGProc, PGType}
import org.postgresql.jdbc2.TypeInfoCache
import org.postgresql.core.Oid

/**
 * User: yuri
 * Date: 4/14/13
 * Time: 5:36 PM 
 */
class PGCatalogTests extends FreeSpec with ShouldMatchers {


  "PGCatalog" - {

    "should create and drop a random schema" in new DBFixture {

      val list = PGCatalog.namespaces(Seq(schema))

      list should have size (1)
      list.head.nspname should equal(schema)

    }.cleanUp

    "should retrieve types" in new DBFixture {

      exec(
        s"create table table_type_1(id int, str text)",
        s"create type enum_type_1 as enum ('a', 'b')"
      )

      val types = PGCatalog.types(Seq(schemaOid))

      types should have size (4)
      //think about array types

      val fTypes = types.collect {
        case t@PGType(_, "table_type_1", _, _, _, 'c', 'C', _) => t
        case t@PGType(_, "enum_type_1", _, _, _, 'e', 'E', _) => t
      }

      fTypes should have size (2)

    }.cleanUp

    "should match procedures" in new DBFixture {

      implicit class It(sql: String) {

        def proc(sql: String): PGProc = {
          val schema = random
          createSchema(schema)
          setSearchPath(schema)

          val oid = PGCatalog.namespaces(Seq(schema)).head.oid

          exec(sql)

          val proc = PGCatalog.procs(Seq(oid)).head

          dropSchema(schema)

          proc
        }

        def shouldEq(expected: PGProc) = {
          val it = proc(sql)
          it should equal((expected.copy(oid = it.oid, pronamespace = it.pronamespace)))
        }
      }

      "create function f() returns int as 'select 1' language sql; " shouldEq PGProc(-1, "f", -1, 0, false, 0, 0, Oid.INT4, Nil, Nil, Nil, Nil, Nil)
      "create function f() returns setof int as 'select 1' language sql; " shouldEq PGProc(-1, "f", -1, 0, true, 0, 0, Oid.INT4, Nil, Nil, Nil, Nil, Nil)
      "create function f(a int, b int) returns setof int as 'select 1' language sql; " shouldEq PGProc(-1, "f", -1, 0, true, 2, 0, Oid.INT4, Seq(Oid.INT4, Oid.INT4), Nil, Nil, Seq("a", "b"), Nil)

    }.cleanUp

  }


}
