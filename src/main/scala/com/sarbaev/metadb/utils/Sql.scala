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


  implicit class RichResultSet(rs: ResultSet){

    def getIntArray(name: String): Seq[Int] = {
      val arr = rs.getArray(name)
        if(rs.wasNull()){
          Seq()
        }else{
          arr.getArray.asInstanceOf[Array[Integer]].map(_.toInt).toList
        }
    }

    def getCharArray(name: String): Seq[Char] = {
      val arr = rs.getArray(name)
      if(rs.wasNull()){
        Seq()
      }else{
        arr.getArray.asInstanceOf[Array[Character]].map(_.toChar).toList
      }
    }

    def getStringArray(name: String): Seq[String] = {
      val arr = rs.getArray(name)
      if(rs.wasNull()){
        Seq()
      }else{
        arr.getArray.asInstanceOf[Array[String]].map(_.toString).toList
      }
    }



  }

}
