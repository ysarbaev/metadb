package com.sarbaev.metadb.test

import org.scalatools.testing.{Fingerprint, TestFingerprint}

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
