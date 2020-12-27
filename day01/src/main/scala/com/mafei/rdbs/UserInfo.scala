/*
 * @Author mafei0728
 * @Description  $end$
 * @Date $time$ $date$
 * @Param $param$
 * @return $return$
 */

package com.mafei.rdbs
import scala.beans.BeanProperty

case class UserInfo() {
  @BeanProperty var name: String = _
  @BeanProperty var age: Int = _

  def this(name: String, age: Int) {
    this()
    this.name = name
    this.age = age
  }




}
