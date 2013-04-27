package com.sarbaev.metadb.test.postgresql

import org.scalatest.FreeSpec
import org.scalatest.matchers.ShouldMatchers
import java.sql.Types

/**
 * User: yuri
 * Date: 4/14/13
 * Time: 9:53 PM 
 */
class PGExamples extends FreeSpec with ShouldMatchers {


  "PG SP Examples" - {

    "call basic procedure" in new PGSchemaFixture {
      closeAfter {

        exec("create function it(a int) returns int as 'select $1*$1' language sql;")

        val param = 42

        val call = connection.prepareCall(s"{? = call it(?) }")
        call.registerOutParameter(1, Types.INTEGER)
        call.setInt(2, param)

        val res = call.execute

        res should be(false)
        call.getInt(1) should equal(param * param)

      }
    }


    "map an array of int" in new PGSchemaFixture {
      closeAfter {


        val stmt = connection.prepareStatement("select '{1,2,3}'::integer[3] as a")

        val rs = stmt.executeQuery
        rs.next should equal(true)

        val array = rs.getArray("a")

        array.getBaseType shouldBe Types.INTEGER
        array.getArray should equal(Array(1, 2, 3))

      }
    }


    "map an empty array" in new PGSchemaFixture {
      closeAfter {

        val stmt = connection.prepareStatement("select '{}'::integer[0] as a")

        val rs = stmt.executeQuery
        rs.next should equal(true)

        val array = rs.getArray("a")

        array.getBaseType shouldBe Types.INTEGER
        array.getArray should equal(Array())

      }
    }


  }
}
