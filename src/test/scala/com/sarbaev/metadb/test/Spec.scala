package com.sarbaev.metadb.test

/**
 * User: yuri
 * Date: 5/12/13
 * Time: 10:50 PM 
 */
trait Spec {

  implicit class TestWrapper(name: String){

    def in[T](f: DBFixture){
//      tests +=  (name -> f)
    }

  }

}
