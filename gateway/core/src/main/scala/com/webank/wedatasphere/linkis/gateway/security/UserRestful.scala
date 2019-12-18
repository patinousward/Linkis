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

import com.webank.wedatasphere.linkis.common.utils.{RSAUtils, Utils}
import com.webank.wedatasphere.linkis.gateway.config.GatewayConfiguration
import com.webank.wedatasphere.linkis.gateway.http.GatewayContext
import com.webank.wedatasphere.linkis.gateway.security.sso.SSOInterceptor
import com.webank.wedatasphere.linkis.server.conf.ServerConfiguration
import com.webank.wedatasphere.linkis.server.security.SSOUtils
import com.webank.wedatasphere.linkis.server.{Message, _}
import org.apache.commons.lang.StringUtils

/**
  * created by cooperyang on 2019/1/9.
  */
trait UserRestful {

  def doUserRequest(gatewayContext: GatewayContext): Unit

}
abstract class AbstractUserRestful extends UserRestful {

  private var securityHooks: Array[SecurityHook] = Array.empty

  def setSecurityHooks(securityHooks: Array[SecurityHook]): Unit = this.securityHooks = securityHooks

  private val userRegex = {
    var userURI = ServerConfiguration.BDP_SERVER_USER_URI.getValue
    if(!userURI.endsWith("/")) userURI += "/"
    userURI
  }

  override def doUserRequest(gatewayContext: GatewayContext): Unit = {
    //获取request中的requestUrI 判断是user的login请求,还是logout请求,亦或是别的
    val path = gatewayContext.getRequest.getRequestURI.replace(userRegex, "")
    val message = path match {
      case "login" =>
        Utils.tryCatch {
          val loginUser = GatewaySSOUtils.getLoginUsername(gatewayContext)
          Message.error(loginUser + "Already logged in, please log out before signing in(已经登录，请先退出再进行登录)！").data("redirectToIndex", true)
          //getLoginUsername抛异常后才调用的login方法
          //不抛异常说明用户已经登陆过,而且没过期
        }(_ => login(gatewayContext))
      case "logout" => logout(gatewayContext)
      case "userInfo" => userInfo(gatewayContext) //获取当前ticket的user名(需要是登陆过的) 好像现在没用上
      case "publicKey" => publicKey(gatewayContext)//获取加密的publickey(现在也是没用上)
      case "heartbeat" => heartbeat(gatewayContext)
      case _ =>
        Message.error("unknown request URI " + path)
    }
    gatewayContext.getResponse.write(message)
    gatewayContext.getResponse.setStatus(Message.messageToHttpStatus(message))
    gatewayContext.getResponse.sendResponse()
  }

  def login(gatewayContext: GatewayContext): Message = {
    //ldap登陆--->写入cookie
    val message = tryLogin(gatewayContext)
    //调用hook
    if(securityHooks != null) securityHooks.foreach(_.postLogin(gatewayContext))
    message
  }

  protected def tryLogin(context: GatewayContext): Message

  def logout(gatewayContext: GatewayContext): Message = {
    //缓存中remove这个用户
    GatewaySSOUtils.removeLoginUser(gatewayContext)
    //如果有sso拦截器的话,(当然当前是没有默认实现的)调用拦截器的logout方法
    if(GatewayConfiguration.ENABLE_SSO_LOGIN.getValue) SSOInterceptor.getSSOInterceptor.logout(gatewayContext)
    if(securityHooks != null) securityHooks.foreach(_.preLogout(gatewayContext))
    "Logout successful(退出登录成功)！"
  }

  def userInfo(gatewayContext: GatewayContext): Message = {
    "get user information succeed!".data("userName", GatewaySSOUtils.getLoginUsername(gatewayContext))
  }

  def publicKey(gatewayContext: GatewayContext): Message = {
    val message = Message.ok("Gain success(获取成功)！").data("enable", SSOUtils.sslEnable)
    //如果允许获取PublicKey的话,就返回给前台,否则只是返回个成功
    if(SSOUtils.sslEnable) message.data("publicKey", RSAUtils.getDefaultPublicKey())
    message
  }

  def heartbeat(gatewayContext: GatewayContext): Message = Utils.tryCatch {
    //在登陆了的用户的缓存中获取用户名,并且如果登陆时间超过期的一半就进行刷新
    //如果抛了异常,或则过期了,就直接抛错,抛未登陆异常
    GatewaySSOUtils.getLoginUsername(gatewayContext)
    "Maintain heartbeat success(维系心跳成功)！"
  }(t => Message.noLogin(t.getMessage))
}
abstract class UserPwdAbstractUserRestful extends AbstractUserRestful {

  override protected def tryLogin(gatewayContext: GatewayContext): Message = {
    val userNameArray = gatewayContext.getRequest.getQueryParams.get("userName")
    val passwordArray = gatewayContext.getRequest.getQueryParams.get("password")
    //get请求中获取用户名和密码
    val (userName, password) = if(userNameArray != null && userNameArray.nonEmpty &&
      passwordArray != null && passwordArray.nonEmpty)
      (userNameArray.head, passwordArray.head)
      //post请求中获取用户名和密码
    else if(StringUtils.isNotBlank(gatewayContext.getRequest.getRequestBody)){
      val json = BDPJettyServerHelper.gson.fromJson(gatewayContext.getRequest.getRequestBody, classOf[java.util.Map[String, Object]])
      (json.get("userName"), json.get("password"))
    } else (null, null)//否则最后就是null 和null
    if(userName == null || StringUtils.isBlank(userName.toString)) {
      Message.error("Username can not be empty(用户名不能为空)！")
    } else if(password == null || StringUtils.isBlank(password.toString)) {
      Message.error("Password can not be blank(密码不能为空)！")
    } else {
      //warn: For easy to useing linkis,Admin skip login
      //如果是admin进行登陆,则直接登陆成功,不用经过ldap
      if(GatewayConfiguration.ADMIN_USER.getValue.equals(userName.toString) && userName.toString.equals(password.toString)){
          GatewaySSOUtils.setLoginUser(gatewayContext, userName.toString)
          "login successful(登录成功)！".data("userName", userName)
            .data("isAdmin", true)
      } else {
        val lowerCaseUserName = userName.toString.toLowerCase
        //登陆方法的调用,这里根据相应的实现类进行,默认是ldap
        val message = login(lowerCaseUserName, password.toString)
        //登陆成功则写入缓存,并且将cookie返回
        if(message.getStatus == 0) GatewaySSOUtils.setLoginUser(gatewayContext, lowerCaseUserName)
        message
      }
    }
  }

  protected def login(userName: String, password: String): Message

}