package com.sarbaev.metadb.test.it

import org.scalatest.FreeSpec
import org.scalatest.matchers.ShouldMatchers
import java.sql.{Types, ResultSet, PreparedStatement, Driver}

/**
 * User: yuri
 * Date: 4/14/13
 * Time: 9:53 PM 
 */
class ClientExampleTests extends FreeSpec with ShouldMatchers{

  "PG SP Examples" - {

    "call basic procedure" in new DBFixture {

      exec("create function it(a int) returns int as 'select $1*$1' language sql;")

      val param = 42

      val call = connection.prepareCall(s"{? = call it(?) }")
      call.registerOutParameter(1, Types.INTEGER)
      call.setInt(2, param)

      val res = call.execute

      res should be(false)
      call.getInt(1) should equal(param * param)



    }.cleanUp

  }

}
