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

    def intArr(name: String): Seq[Int] = {
      val arr = rs.getArray(name)
        if(rs.wasNull()){
          Seq()
        }else{
          arr.getArray.asInstanceOf[Array[Integer]].map(_.toInt).toList
        }
    }

    def charArr(name: String): Seq[Char] = {
      val arr = rs.getArray(name)
      if(rs.wasNull()){
        Seq()
      }else{
        arr.getArray.asInstanceOf[Array[Character]].map(_.toChar).toList
      }
    }

    def strArr(name: String): Seq[String] = {
      val arr = rs.getArray(name)
      if(rs.wasNull()){
        Seq()
      }else{
        arr.getArray.asInstanceOf[Array[String]].map(_.toString).toList
      }
    }

    def int(name: String) = rs getInt name
    def str(name: String) = rs getString name
    def bool(name: String) = rs getBoolean name
    def char(name: String): Character = {
      val str = rs getString name
      if(str != null && str.length > 0){
        str.charAt(0)
      }else{
        null
      }
    }
  }

}
