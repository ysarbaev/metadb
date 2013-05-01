package com.sarbaev.metadb

import scala.reflect.macros.Context
import language.experimental.macros

/**
 * User: yuri
 * Date: 3/31/13
 * Time: 11:47 PM 
 */
object MetaDB {

  def impl(c: Context)(url: c.Expr[String],
                       user: c.Expr[String],
                       password: c.Expr[String],
                       driver: c.Expr[String],
                       tableNamespaces: c.Expr[Set[String]],
                       functionNamespaces: c.Expr[Set[String]]
    ) = {

    import c.universe._

    reify {
      println(url.splice)
    }
  }

  type DB(url: String, user: String, password: String, driver: String, tableNamespaces: Set[String], functionNamespaces: Set[String]) = macro impl
}
