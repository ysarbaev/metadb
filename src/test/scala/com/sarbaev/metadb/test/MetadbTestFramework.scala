package com.sarbaev.metadb.test

import com.sarbaev.metadb.test.postgresql.{PGExamples, PGCatalogTests}

/**
 * User: yuri
 * Date: 5/12/13
 * Time: 10:56 PM 
 */
object MetadbTestFramework extends App {
  def specs = Array(
    new PGCatalogTests,
    new PGExamples
  )

  override def main(args: Array[String]) {
    specs
  }

}
