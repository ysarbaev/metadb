package com.sarbaev.metadb.utils

import java.sql.ResultSet

/**
 * User: yuri
 * Date: 4/8/13
 * Time: 10:13 PM 
 */
object Sql {

  implicit class ResultSetIterator(rs: ResultSet) extends Iterable[ResultSet] {
    def iterator = new Iterator[ResultSet]() {
      def hasNext: Boolean = !rs.isLast

      def next() = {
        rs.next
        rs
      }
    }
  }

}
