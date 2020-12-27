package com.mafei.test

import org.junit.Test

class Demo01 {

  @Test
  def fu01(): Unit = {
    val a1 = Array(1, 2, 3, 4)
    val a2 = Array(3, 4, 54, 5)
    val r = Array(a1, a2)
    println(Array.concat(r: _*).toList)
  }
}
