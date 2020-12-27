package com.mafei.access

import java.sql.Timestamp
import scala.beans.BeanProperty


class UserInfo {
  @BeanProperty var id: Int = _
  @BeanProperty var name: String = _
  @BeanProperty var address: String = _
  @BeanProperty var tdate: Timestamp = _

  def this(id: Int, name: String, address: String, tdate: Timestamp) {
    this()
    this.id = id
    this.name = name
    this.address = address
    this.tdate = tdate
  }

  override def hashCode(): Int = super.hashCode()




  def canEqual(other: Any): Boolean = other.isInstanceOf[UserInfo]

  override def equals(other: Any): Boolean = other match {
    case that: UserInfo =>
      (that canEqual this) &&
        id == that.id &&
        name == that.name &&
        address == that.address &&
        tdate == that.tdate
    case _ => false
  }

  override def toString = s"UserInfo($id, $name, $address, $tdate)"

}
