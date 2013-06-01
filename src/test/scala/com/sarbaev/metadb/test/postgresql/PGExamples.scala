package com.sarbaev.metadb.test.postgresql

import java.sql.Types
import com.sarbaev.metadb.test.Spec

/**
 * User: yuri
 * Date: 4/14/13
 * Time: 9:53 PM
 */
class PGExamples extends Spec {


  "call basic procedure" in new PGSchemaFixture {
    closeAfter {

      exec("create function it(a int) returns int as 'select $1*$1' language sql;")

      val param = 42

      val call = connection.prepareCall(s"{? = call it(?) }")
      call.registerOutParameter(1, Types.INTEGER)
      call.setInt(2, param)

      val res = call.execute

      assert(res == false)
      assert(call.getInt(1) == param * param)

    }
  }


  "map an array of int" in new PGSchemaFixture {
    closeAfter {


      val stmt = connection.prepareStatement("select '{1,2,3}'::integer[3] as a")

      val rs = stmt.executeQuery
      assert(rs.next == true)

      val array = rs.getArray("a")

      assert(array.getBaseType == Types.INTEGER)

    }
  }


  "map an empty array" in new PGSchemaFixture {
    closeAfter {

      val stmt = connection.prepareStatement("select '{}'::integer[0] as a")

      val rs = stmt.executeQuery
      assert(rs.next == true)

      val array = rs.getArray("a")

      assert(array.getBaseType == Types.INTEGER)

    }
  }

}
