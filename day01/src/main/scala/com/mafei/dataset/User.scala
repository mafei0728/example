/*
 * @Author mafei0728
 * @Description  $end$
 * @Date $time$ $date$
 * @Param $param$
 * @return $return$
 */

package com.mafei.dataset

import scala.beans.BeanProperty

class User() {
  @BeanProperty var name: String = _
  @BeanProperty var age: Int = _

  def this(name: String, age: Int) {
    this()
    this.name = name
    this.age = age
  }





}
