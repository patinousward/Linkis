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

package com.webank.wedatasphere.linkis.gateway.security

import com.webank.wedatasphere.linkis.common.utils.{Logging, Utils}
import com.webank.wedatasphere.linkis.gateway.http.{GatewayContext, GatewayHttpRequest}
import com.webank.wedatasphere.linkis.server.exception.LoginExpireException
import com.webank.wedatasphere.linkis.server.security.{SSOUtils, ServerSSOUtils}
import com.webank.wedatasphere.linkis.server.security.SecurityFilter._
import javax.servlet.http.Cookie

import scala.collection.JavaConversions._

/**
  * created by cooperyang on 2019/1/9.
  */
//这里sso只是个统称,ldap也是sso的一种(主要使用的方法)
object GatewaySSOUtils extends Logging {
  private def getCookies(gatewayContext: GatewayContext): Array[Cookie] = gatewayContext.getRequest.getCookies.flatMap(_._2).toArray
  def getLoginUser(gatewayContext: GatewayContext): Option[String] = {
    val cookies = getCookies(gatewayContext)
    Utils.tryCatch(SSOUtils.getLoginUser(cookies)) {
      case _: LoginExpireException if Option(cookies).exists(_.exists(c => c.getName == ALLOW_ACCESS_WITHOUT_TIMEOUT && c.getValue == "true")) =>
        ServerSSOUtils.getLoginUserIgnoreTimeout(key => Option(cookies).flatMap(_.find(_.getName == key).map(_.getValue))).filter(_ != OTHER_SYSTEM_IGNORE_UM_USER)
      case t => throw t
    }
  }
  def getLoginUsername(gatewayContext: GatewayContext): String = SSOUtils.getLoginUsername(getCookies(gatewayContext))
  def setLoginUser(gatewayContext: GatewayContext, username: String): Unit = {
    //ProxyUserUtils.getProxyUser(username)
    //proxy.properties中有代理用户名,就返回代理用户名,否则返回原来的名
    val proxyUser = ProxyUserUtils.getProxyUser(username)
    //主要是将username,时间戳 进行加密,加密后 放入userTicketIdToLastAccessTime缓存中
    //然后将加密后的ticketid  写入cookie并且返回
    SSOUtils.setLoginUser(c => gatewayContext.getResponse.addCookie(c), proxyUser)
  }
  def setLoginUser(request: GatewayHttpRequest, username: String): Unit = {
    val proxyUser = ProxyUserUtils.getProxyUser(username)
    SSOUtils.setLoginUser(c => request.addCookie(c.getName, Array(c)), proxyUser)
  }
  def removeLoginUser(gatewayContext: GatewayContext): Unit = {
    //移除缓存userTicketIdToLastAccessTime
    SSOUtils.removeLoginUser(gatewayContext.getRequest.getCookies.flatMap(_._2).toArray)
    //移除浏览器的cookie
    SSOUtils.removeLoginUserByAddCookie(c => gatewayContext.getResponse.addCookie(c))
  }

  //单纯更新缓存userTicketIdToLastAccessTime中的时间
  def updateLastAccessTime(gatewayContext: GatewayContext): Unit = SSOUtils.updateLastAccessTime(gatewayContext.getRequest.getCookies.flatMap(_._2).toArray)
}
