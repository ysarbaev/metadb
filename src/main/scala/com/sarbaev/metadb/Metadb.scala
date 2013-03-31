package com.sarbaev.metadb

import scala.reflect.macros.Context
import language.experimental.macros

/**
 * User: yuri
 * Date: 3/31/13
 * Time: 11:47 PM 
 */
object Metadb {

  def impl(c: Context)(url: c.Expr[String],
                       login: c.Expr[String],
                       password: c.Expr[String],
                       tableSchemas: c.Expr[Set[String]],
                       functionSchemas: c.Expr[Set[String]],
                       driver: c.Expr[Class[java.sql.Driver]]) = {

    import c.universe._

    reify {println(url.splice)}
  }


  def meta(url: String, login: String, password: String, tableSchemas: Set[String], functionSchemas: Set[String], driver: Class[java.sql.Driver]) = macro impl

}
