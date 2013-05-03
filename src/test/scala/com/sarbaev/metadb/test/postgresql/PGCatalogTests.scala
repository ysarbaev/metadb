package com.sarbaev.metadb.test.postgresql

import org.scalatest.FreeSpec
import com.sarbaev.metadb.postgresql.PGCatalog
import org.scalatest.matchers.ShouldMatchers
import com.sarbaev.metadb.postgresql.PGCatalog.{PGDefaultValue, PGProc, PGType}
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

        val list = PGCatalog.PGNamespace.list(Seq(schema))

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

        val types = PGCatalog.PGType.list(Seq(schemaOid))

        types should have size (4)
        //think about array types

        val fTypes = types.collect {
          case t@PGType(_, "table_type_1", _, _, _, 'c', 'C', relid, 0, _,  _) if relid > 0 => t
          case t@PGType(_, "enum_type_1", _, _, _, 'e', 'E',0, 0, _, _) => t
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

            val oid = PGCatalog.PGNamespace.list(Seq(schema)).head.oid

            exec(sql)

            val proc = PGCatalog.PGProc.list(Seq(oid)).head

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

        "create function f(a int, b int default 42) returns int as 'select 1' language sql; " shouldEq
          PGProc(-1, "f", -1, 0, false, 2, 1, Oid.INT4, Seq(Oid.INT4, Oid.INT4), Nil, Nil, Seq("a", "b"), Nil)

        "create function f(int, int) returns int as 'select 1' language sql; " shouldEq
          PGProc(-1, "f", -1, 0, false, 2, 0, Oid.INT4, Seq(Oid.INT4, Oid.INT4), Nil, Nil, Nil, Nil)
      }
    }

    "should retrieve classes" in new PGSchemaFixture {
      closeAfter {

        exec(
          "create table t (a int, b text)",
          "create view v as select * from t"
        )

        val classes = PGCatalog.PGClass.list(Seq(schemaOid))

        classes should have size (2)

      }
    }

    "should retrieve attributes" in new PGSchemaFixture {
      closeAfter {
        exec("create table t (a int, b text, c timestamp, d boolean )")

        val attributes = PGCatalog.PGAttribute.list(Seq(schemaOid))

        attributes should have size (4)

      }
    }

    "should retrieve primary key constraints" in new PGSchemaFixture {
      closeAfter {
        exec(
          """
              create table t1 (a int, b int primary key);
              create table t2 (a int, b int, c int, primary key(a, b, c));
              create table t3 (a int);
              alter table t3 add constraint t3_pk primary key(a);
          """
        )

        val constraints = PGCatalog.PGConstraint.primaryKeys(Seq(schemaOid))

        constraints should have size(3)
      }
    }

    "should retrieve default attributes" in new PGSchemaFixture {
      closeAfter {

        exec("create table t (a int default 42, b text default 'hello')")

        val defaultAttributes = PGCatalog.PGAttributeDefault.list(Seq(schemaOid))

        defaultAttributes should have size(2)
      }
    }

    "should parse default parameters" in {

      val params =
        """({CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 33 :constvalue 4 [ 10 0 0 0 0 0 0 0 ]}
          | {CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 :constbyval false :constisnull false :location 52 :constvalue 4 [ 16 0 0 0 ]}
          | {CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 :constbyval false :constisnull true :location 71 :constvalue <>})""".stripMargin

      val actual = PGCatalog.parseDefault(params)

      val expected = Seq(
        PGDefaultValue(23, -1, 0, 4, true, false, 33, None),
        PGDefaultValue(25, -1, 100, -1, false, false, 52, None),
        PGDefaultValue(25, -1, 100, -1, false, true, 71, None)
      )

      actual shouldBe expected

    }
  }
}
