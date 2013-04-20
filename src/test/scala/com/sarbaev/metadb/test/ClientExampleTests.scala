package com.sarbaev.metadb.test

import org.scalatest.FreeSpec
import org.scalatest.matchers.ShouldMatchers
import java.sql.Types
import com.sarbaev.metadb.utils.Sql.ResultSetIterator
import org.postgresql.jdbc2.AbstractJdbc2Array

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


    "map an array of int" in new DBFixture {

      exec(
        "create table t (a integer[3]);",
        "insert into t(a) values('{1,2,3}');"
      )

      val stmt = connection.prepareStatement("select a from t")

      val rs = stmt.executeQuery
      rs.next should equal(true)

      val array = rs.getArray("a")

      array.getBaseType shouldBe Types.INTEGER
      array.getResultSet.map(_.getInt(2)).toSeq shouldBe Seq(1,2,3)

    }.cleanUp

  }

}
