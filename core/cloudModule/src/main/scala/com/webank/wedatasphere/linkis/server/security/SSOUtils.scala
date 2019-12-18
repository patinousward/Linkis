/*
 * Copyright 2019 WeBank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.wedatasphere.linkis.server.security

import java.util
import java.util.concurrent.TimeUnit

import com.webank.wedatasphere.linkis.common.conf.Configuration
import com.webank.wedatasphere.linkis.common.utils.{Logging, RSAUtils, Utils}
import com.webank.wedatasphere.linkis.server.conf.ServerConfiguration
import com.webank.wedatasphere.linkis.server.exception.{IllegalUserTicketException, LoginExpireException, NonLoginException}
import javax.servlet.http.Cookie

import scala.collection.JavaConversions

object SSOUtils extends Logging {

  private[security] val USER_TICKET_ID_STRING = "bdp-user-ticket-id"
  private val sessionTimeout = ServerConfiguration.BDP_SERVER_WEB_SESSION_TIMEOUT.getValue.toLong
  //用来存储用户登陆的缓存信息,String是ticketid,value是lastAccessTime
  private val userTicketIdToLastAccessTime = new util.HashMap[String, Long]()
  val sslEnable: Boolean = ServerConfiguration.BDP_SERVER_SECURITY_SSL.getValue
  def decryptLogin(passwordString: String): String = if(sslEnable) {
    new String(RSAUtils.decrypt(passwordString), Configuration.BDP_ENCODING.getValue)
  } else passwordString

  Utils.defaultScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = JavaConversions.mapAsScalaMap(userTicketIdToLastAccessTime).filter(System.currentTimeMillis - _._2 > sessionTimeout).foreach {
      case (k, v) => if(userTicketIdToLastAccessTime.containsKey(k)) userTicketIdToLastAccessTime synchronized {
        if(userTicketIdToLastAccessTime.containsKey(k) && System.currentTimeMillis - userTicketIdToLastAccessTime.get(k) > sessionTimeout) {
          info(s"remove timeout userTicket $k, since the last access time is $v.")
          userTicketIdToLastAccessTime.remove(k)
        }
      }
    }
  }, sessionTimeout, sessionTimeout/10, TimeUnit.MILLISECONDS)

  private[security] def getUserAndLoginTime(userTicketId: String): Option[(String, Long)] = {
    //getUsernameByTicket返回的格式应该是username,过期时间
    ServerConfiguration.getUsernameByTicket(userTicketId).map { userAndLoginTime =>
      if(userAndLoginTime.indexOf(",") < 0) throw new IllegalUserTicketException(s"Illegal user token information(非法的用户token信息).")
      val index = userAndLoginTime.lastIndexOf(",")
      (userAndLoginTime.substring(0, index), userAndLoginTime.substring(index + 1).toLong)
    }
  }

  //Determine the unique ID by username and timestamp(通过用户名和时间戳，确定唯一ID)
  private def getUserTicketId(username: String): String = {
    val timeoutUser = username + "," + System.currentTimeMillis
    ServerConfiguration.getTicketByUsername(timeoutUser)
  }

  def setLoginUser(addCookie: Cookie => Unit, username: String): Unit = {
    info(s"add login userTicketCookie for user $username.")
    val userTicketId = getUserTicketId(username)
    userTicketIdToLastAccessTime.put(userTicketId, System.currentTimeMillis())
    val cookie = new Cookie(USER_TICKET_ID_STRING, userTicketId)
    cookie.setMaxAge(-1)
    if(sslEnable) cookie.setSecure(true)
    cookie.setPath("/")
    addCookie(cookie)
  }

  def setLoginUser(addUserTicketKV: (String, String) => Unit, username: String): Unit = {
    info(s"add login userTicket for user $username.")
    val userTicketId = getUserTicketKV(username)
    userTicketIdToLastAccessTime.put(userTicketId._2, System.currentTimeMillis())
    addUserTicketKV(userTicketId._1, userTicketId._2)
  }

  private[linkis] def getUserTicketKV(username: String): (String, String) = {
    val userTicketId = getUserTicketId(username)
    (USER_TICKET_ID_STRING, userTicketId)
  }

  def removeLoginUser(getCookies: => Array[Cookie]): Unit = {
    val cookies = getCookies
    if(cookies != null) cookies.find(_.getName == USER_TICKET_ID_STRING).foreach { cookie =>
      if(userTicketIdToLastAccessTime.containsKey(cookie.getValue)) userTicketIdToLastAccessTime synchronized {
        if(userTicketIdToLastAccessTime.containsKey(cookie.getValue)) userTicketIdToLastAccessTime.remove(cookie.getValue)
      }
      cookie.setValue(null)
      cookie.setMaxAge(0)
    }
  }

  def removeLoginUserByAddCookie(addEmptyCookie: Cookie => Unit): Unit = {
    val cookie = new Cookie(USER_TICKET_ID_STRING, null)
    cookie.setMaxAge(0)
    cookie.setPath("/")
    if(sslEnable) cookie.setSecure(true)
    addEmptyCookie(cookie)
  }

  def removeLoginUser(removeKeyReturnValue: String => Option[String]): Unit = removeKeyReturnValue(USER_TICKET_ID_STRING).foreach{ t =>
    if(userTicketIdToLastAccessTime.containsKey(t)) userTicketIdToLastAccessTime synchronized {
      if(userTicketIdToLastAccessTime.containsKey(t)) userTicketIdToLastAccessTime.remove(t)
    }
  }

  def getLoginUsername(getCookies: => Array[Cookie]): String = getLoginUser(getCookies).getOrElse(throw new NonLoginException(s"You are not logged in, please login first(您尚未登录，请先登录!)"))

  //: => 其实=>可以省略吧  单纯就是需要传入一个Array[Cookie] 并非是传入一个函数
  //_ => Option(getCookies... 其实是x:String =>Option(getCookies...的简写
  //_表示这个函数的实现类是一个无论x的传入是什么,最后都返回一个 Option(getCookies的函数
  def getLoginUser(getCookies: => Array[Cookie]): Option[String] = getLoginUser(_ => Option(getCookies).flatMap(_.find(_.getName == USER_TICKET_ID_STRING).map(_.getValue)))
  //这里getUserTicketId 传入的参数对上面getLoginUser方法调用来说就是没啥用,可能别的地方会使用到
  def getLoginUser(getUserTicketId: String => Option[String]): Option[String] =
    getUserTicketId(USER_TICKET_ID_STRING).map { t =>
      //这里t是bdp-user-ticket-id 对应的value
    isTimeoutOrNot(t)//检验是否过期
    getUserAndLoginTime(t).getOrElse(throw new IllegalUserTicketException( s"Illegal user token information(非法的用户token信息)."))._1
  }

  def getLoginUsername(getUserTicketId: String => Option[String]): String = getLoginUser(getUserTicketId).getOrElse(throw new NonLoginException(s"You are not logged in, please login first(您尚未登录，请先登录!)"))
//将获取的cookie得到(username,logintime)一个元组,最后获取元组的第一个
  private[security] def getLoginUserIgnoreTimeout(getUserTicketId: String => Option[String]): Option[String] =
    getUserTicketId(USER_TICKET_ID_STRING).map(getUserAndLoginTime).flatMap(_.map(_._1))

  def updateLastAccessTime(getCookies: => Array[Cookie]): Unit = updateLastAccessTime(_ => Option(getCookies).flatMap(_.find(_.getName == USER_TICKET_ID_STRING).map(_.getValue)))

  def updateLastAccessTime(getUserTicketId: String => Option[String]): Unit = getUserTicketId(USER_TICKET_ID_STRING).foreach(isTimeoutOrNot)

  private def isTimeoutOrNot(userTicketId: String): Unit = if(!userTicketIdToLastAccessTime.containsKey(userTicketId)) {
    throw new LoginExpireException("You are not logged in, please login first!(您尚未登录，请先登录!)")
  } else {
    val lastAccessTime = userTicketIdToLastAccessTime.get(userTicketId)
    if(System.currentTimeMillis - lastAccessTime > sessionTimeout && !Configuration.IS_TEST_MODE.getValue) userTicketIdToLastAccessTime synchronized {
      if(userTicketIdToLastAccessTime.containsKey(userTicketId) && System.currentTimeMillis - userTicketIdToLastAccessTime.get(userTicketId) > sessionTimeout) {
        userTicketIdToLastAccessTime.remove(userTicketId)
        throw new LoginExpireException("Login has expired, please log in again!(登录已过期，请重新登录！)")
      }
      //如果过期了一般的实现,重新更新下access的时间
    } else if(System.currentTimeMillis - lastAccessTime >= sessionTimeout * 0.5) {
      userTicketIdToLastAccessTime.put(userTicketId, System.currentTimeMillis)
    }
  }

}
