package com.sarbaev.metadb.test.postgresql

import org.scalatest.FreeSpec
import com.sarbaev.metadb.postgresql.PGCatalog
import org.scalatest.matchers.ShouldMatchers
import com.sarbaev.metadb.postgresql.PGCatalog.{PGProcDefaultParameter, PGProc, PGType}
import org.postgresql.core.Oid

/**
 * User: yuri
 * Date: 4/14/13
 * Time: 5:36 PM 
 */
class PGCatalogTests extends FreeSpec with ShouldMatchers {

  "PGCatalog" - {

    "should create and drop a random schema" in new PGSchemaFixture {
      closeAfter {

        val list = PGCatalog.namespaces(Seq(schema))

        list should have size (1)
        list.head.nspname should equal(schema)

      }
    }

    "should retrieve types" in new PGSchemaFixture {
      closeAfter {

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

      }
    }

    /**
     * -1 means undetermined
     */
    "should match procedures" in new PGSchemaFixture {
      closeAfter {

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
            withClue(sql) {
              it shouldBe (expected.copy(oid = it.oid, pronamespace = it.pronamespace))
            }
          }
        }

        "create function f() returns int as 'select 1' language sql; " shouldEq
          PGProc(-1, "f", -1, 0, false, 0, 0, Oid.INT4, Nil, Nil, Nil, Nil, Nil)

        "create function f() returns setof int as 'select 1' language sql; " shouldEq
          PGProc(-1, "f", -1, 0, true, 0, 0, Oid.INT4, Nil, Nil, Nil, Nil, Nil)

        "create function f(a int, b int) returns int as 'select 1' language sql; " shouldEq
          PGProc(-1, "f", -1, 0, false, 2, 0, Oid.INT4, Seq(Oid.INT4, Oid.INT4), Nil, Nil, Seq("a", "b"), Nil)

        "create function f(a int, b int default 42) returns int as 'select 1' language sql" shouldEq
          PGProc(-1, "f", -1, 0, false, 2, 1, Oid.INT4, Seq(Oid.INT4, Oid.INT4), Nil, Nil, Seq("a", "b"), Seq(PGProcDefaultParameter(23, -1, 0, 4, true, false, 39, None)))

      }
    }

    "should parse default parameters" in {

      val params =
        """({CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 33 :constvalue 4 [ 10 0 0 0 0 0 0 0 ]}
          | {CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 :constbyval false :constisnull false :location 52 :constvalue 4 [ 16 0 0 0 ]}
          | {CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 :constbyval false :constisnull true :location 71 :constvalue <>})""".stripMargin

      val actual = PGCatalog.parseProcDefaultParameters(params)

      val expected = Seq(
        PGProcDefaultParameter(23,-1,0,4,true, false, 33, None),
        PGProcDefaultParameter(25,-1,100,-1,false, false, 52, None),
        PGProcDefaultParameter(25,-1,100,-1,false, true, 71, None)
      )

      actual shouldBe expected

    }
  }
}
