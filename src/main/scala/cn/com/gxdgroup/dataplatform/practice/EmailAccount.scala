package cn.com.gxdgroup.dataplatform.practice

import scala.beans.BeanProperty

/**
 * Created by wq on 14-5-14.
 */
class EmailAccount {
  @BeanProperty var accountName: String = null
  @BeanProperty var username: String = null
  @BeanProperty var password: String = null
  @BeanProperty var mailbox: String = null
  @BeanProperty var imapServerUrl: String = null
  @BeanProperty var minutesBetweenChecks: Int = 0
  @BeanProperty var protocol: String = null
  @BeanProperty var usersOfInterest = new java.util.ArrayList[String]()
}

object EmailAccount {
  val bean = new EmailAccount()
  bean.setAccountName("dd")
  bean.getAccountName
}
