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

import java.text.DateFormat
import java.util.{Date, Locale}

import com.webank.wedatasphere.linkis.common.conf.Configuration
import com.webank.wedatasphere.linkis.common.exception.DWCException
import com.webank.wedatasphere.linkis.common.utils.Utils
import com.webank.wedatasphere.linkis.gateway.config.GatewayConfiguration
import com.webank.wedatasphere.linkis.gateway.http.GatewayContext
import com.webank.wedatasphere.linkis.gateway.security.sso.SSOInterceptor
import com.webank.wedatasphere.linkis.gateway.security.token.TokenAuthentication
import com.webank.wedatasphere.linkis.server.conf.ServerConfiguration
import com.webank.wedatasphere.linkis.server.exception.{LoginExpireException, NonLoginException}
import com.webank.wedatasphere.linkis.server.{Message, validateFailed}
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.exception.ExceptionUtils

/**
  * created by cooperyang on 2019/1/9.
  */
//gateway的SecurityFilter是转发的时候进行过滤
// com.webank.wedatasphere.linkis.server.security.SecurityFilter则是直接嵌入到jetty的过滤器中,请求都进行过滤
object SecurityFilter {

  private val refererValidate = ServerConfiguration.BDP_SERVER_SECURITY_REFERER_VALIDATE.getValue
  private val localAddress = ServerConfiguration.BDP_SERVER_ADDRESS.getValue
  protected val testUser: String = ServerConfiguration.BDP_TEST_USER.getValue

  private var userRestful: UserRestful = _
  def setUserRestful(userRestful: UserRestful): Unit = this.userRestful = userRestful

  def filterResponse(gatewayContext: GatewayContext, message: Message): Unit = {
    gatewayContext.getResponse.setStatus(Message.messageToHttpStatus(message))
    gatewayContext.getResponse.write(message)
    gatewayContext.getResponse.sendResponse()
  }
  //这个Filter 是springcloud转发的时候进行调用的,dofilter返回true说明ok,返回false说明不允许访问
  //请求都是在这里做的ticket校验
  def doFilter(gatewayContext: GatewayContext): Boolean = {
    //给response加一些跨域访问的内容
    addAccessHeaders(gatewayContext)
    if(refererValidate) {
      //Security certification support, referer limited(安全认证支持，referer限定)
      val referer = gatewayContext.getRequest.getHeaders.get("Referer")
      if(referer != null && referer.nonEmpty && StringUtils.isNotEmpty(referer.head) && !referer.head.trim.contains(localAddress)) {
        filterResponse(gatewayContext, validateFailed("Unallowed cross-site request(不允许的跨站请求)！"))
        return false
      }
      //Security certification support, solving verb tampering(安全认证支持，解决动词篡改)
      gatewayContext.getRequest.getMethod.toUpperCase match {
        case "GET" | "POST" | "PUT" | "DELETE" | "HEAD" | "TRACE" | "CONNECT" | "OPTIONS" =>
        case _ =>
          filterResponse(gatewayContext, validateFailed("Do not use HTTP verbs to tamper with(不可使用HTTP动词篡改)！"))
          return false
      }
    }
    //isPassAuthRequest免校验的requesturl的记录
    val isPassAuthRequest = GatewayConfiguration.PASS_AUTH_REQUEST_URI.exists(gatewayContext.getRequest.getRequestURI.startsWith)
    //如果是/user/login  logout  等..url的,就进行登陆操作
    //已经登陆会抛出异常
    if(gatewayContext.getRequest.getRequestURI.startsWith(ServerConfiguration.BDP_SERVER_USER_URI.getValue)) {
      Utils.tryCatch(userRestful.doUserRequest(gatewayContext)){ t =>
        val message = t match {
          case dwc: DWCException => dwc.getMessage
          case _ => "login failed! reason: " + ExceptionUtils.getRootCauseMessage(t)
        }
        GatewaySSOUtils.error("login failed!", t)
        filterResponse(gatewayContext, Message.error(message).<<(gatewayContext.getRequest.getRequestURI))
      }
      false
    }
      //如果是免登陆的url地址而且不允许sso登陆,直接不校验返回true
    else if(isPassAuthRequest && !GatewayConfiguration.ENABLE_SSO_LOGIN.getValue) {
      GatewaySSOUtils.info("No login needed for proxy uri: " + gatewayContext.getRequest.getRequestURI)
      true
    }
    //如果是token校验,则直接校验token.properties中的token用户,有不报错,没有抛出异常
    else if(TokenAuthentication.isTokenRequest(gatewayContext)) {
      TokenAuthentication.tokenAuth(gatewayContext)
    } else {
      //剩余的就普通请求,getLoginUser  看下用户有没登陆
      val userName = Utils.tryCatch(GatewaySSOUtils.getLoginUser(gatewayContext)){
        //如果抛出的异常是NonLoginException  或者登陆过期
        case n @ (_: NonLoginException | _: LoginExpireException )=>
          //如果没登陆,但是开启了testmode,就返回None了
          if(Configuration.IS_TEST_MODE.getValue) None else {
            //如果未登陆,也没开启testmode,就返回未登陆的报错
            filterResponse(gatewayContext, Message.noLogin(n.getMessage) << gatewayContext.getRequest.getRequestURI)
            return false
          }
          //其他异常
        case t: Throwable =>
          GatewaySSOUtils.warn("", t)
          throw t
      }
      if(userName.isDefined) {
        true
      } else if(Configuration.IS_TEST_MODE.getValue) {
        //开启testMode就返回wds.linkis.test.user的变量名
        GatewaySSOUtils.info("test mode! login for uri: " + gatewayContext.getRequest.getRequestURI)
        GatewaySSOUtils.setLoginUser(gatewayContext, testUser)
        true
      } else if(GatewayConfiguration.ENABLE_SSO_LOGIN.getValue) {
        //如果允许sso登陆(这里的sso指自定义的sso,而非ldap,但是SSOutils 这个概念倒是包含了ldap登陆)
        val user = SSOInterceptor.getSSOInterceptor.getUser(gatewayContext)
        if(StringUtils.isNotBlank(user)) {
          GatewaySSOUtils.setLoginUser(gatewayContext.getRequest, user)
          true
        }
        //sso也没有或则返回null的话,看下这个url是否是免验证的,免登陆的就重定向
        else if(isPassAuthRequest) {
          gatewayContext.getResponse.redirectTo(SSOInterceptor.getSSOInterceptor.redirectTo(gatewayContext.getRequest.getURI))
          gatewayContext.getResponse.sendResponse()
          false
        } else {
          //其他一律返回false
          filterResponse(gatewayContext, Message.noLogin("You are not logged in, please login first(您尚未登录，请先登录)!")
            .data("enableSSO", true).data("SSOURL", SSOInterceptor.getSSOInterceptor.redirectTo(gatewayContext.getRequest.getURI)) << gatewayContext.getRequest.getRequestURI)
          false
        }
      } else {
        //其他一律返回false
        filterResponse(gatewayContext, Message.noLogin("You are not logged in, please login first(您尚未登录，请先登录)!") << gatewayContext.getRequest.getRequestURI)
        false
      }
    }
  }

  protected def addAccessHeaders(gatewayContext: GatewayContext) {
    val response = gatewayContext.getResponse
    response.setHeader("Access-Control-Allow-Origin", "*")
    response.setHeader("Access-Control-Allow-Credentials", "true")
    response.setHeader("Access-Control-Allow-Headers", "authorization,Content-Type")
    response.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, HEAD, DELETE")
    val fullDateFormatEN = DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL, new Locale("EN", "en"))
    response.setHeader("Date", fullDateFormatEN.format(new Date))
  }

}
