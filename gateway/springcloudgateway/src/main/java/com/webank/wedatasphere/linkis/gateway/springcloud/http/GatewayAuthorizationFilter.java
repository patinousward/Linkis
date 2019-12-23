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

package com.webank.wedatasphere.linkis.gateway.springcloud.http;

import com.webank.wedatasphere.linkis.common.ServiceInstance;
import com.webank.wedatasphere.linkis.common.utils.JavaLog;
import com.webank.wedatasphere.linkis.gateway.exception.GatewayWarnException;
import com.webank.wedatasphere.linkis.gateway.http.BaseGatewayContext;
import com.webank.wedatasphere.linkis.gateway.parser.GatewayParser;
import com.webank.wedatasphere.linkis.gateway.route.GatewayRouter;
import com.webank.wedatasphere.linkis.gateway.security.SecurityFilter;
import com.webank.wedatasphere.linkis.gateway.springcloud.SpringCloudGatewayConfiguration;
import com.webank.wedatasphere.linkis.server.Message;
import org.apache.commons.lang.StringUtils;
import org.springframework.cloud.gateway.config.GatewayProperties;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.cloud.gateway.support.DefaultServerRequest;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.http.server.reactive.AbstractServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

/**
 * created by cooperyang on 2019/1/9.
 */
public class GatewayAuthorizationFilter extends JavaLog implements GlobalFilter, Ordered {

    private GatewayParser parser;
    private GatewayRouter router;
    private GatewayProperties gatewayProperties;

    public GatewayAuthorizationFilter(GatewayParser parser, GatewayRouter router, GatewayProperties gatewayProperties) {
        this.parser = parser;
        this.router = router;
        this.gatewayProperties = gatewayProperties;
    }

    private String getRequestBody(ServerWebExchange exchange) {
//        StringBuilder requestBody = new StringBuilder();
        DefaultServerRequest serverRequest = new DefaultServerRequest(exchange);
        String requestBody = null;
        try {
            requestBody = serverRequest.bodyToMono(String.class).toFuture().get();
        } catch (Exception e) {
            GatewayWarnException exception = new GatewayWarnException(18000, "get requestBody failed!");
            exception.initCause(e);
            throw exception;
        }
        return requestBody;
    }

    private BaseGatewayContext getBaseGatewayContext(ServerWebExchange exchange, Route route) {
        //获取request和response对象
        AbstractServerHttpRequest request = (AbstractServerHttpRequest) exchange.getRequest();
        ServerHttpResponse response = exchange.getResponse();
        //new 一个BaseGatewayContext  GatewayContext也没别的实现类
        BaseGatewayContext gatewayContext = new BaseGatewayContext();
        //将AbstractServerHttpRequest 封装为SpringCloudGatewayHttpRequest
        SpringCloudGatewayHttpRequest springCloudGatewayHttpRequest = new SpringCloudGatewayHttpRequest(request);
        gatewayContext.setRequest(springCloudGatewayHttpRequest);
        //将ServerHttpResponse 封装为SpringCloudGatewayHttpResponse
        //将request和response放入gatewayContext
        gatewayContext.setResponse(new SpringCloudGatewayHttpResponse(response));
        if(route.getUri().toString().startsWith(SpringCloudGatewayConfiguration.ROUTE_URI_FOR_WEB_SOCKET_HEADER())){
            //如果是websocket请求,就将gatewayContext中是否是websocket 字段修改为true
            gatewayContext.setWebSocketRequest();
        }
//        if(!gatewayContext.isWebSocketRequest() && parser.shouldContainRequestBody(gatewayContext)) {
//            String requestBody = getRequestBody(exchange);
//            springCloudGatewayHttpRequest.setRequestBody(requestBody);
//        }
        return gatewayContext;
    }

    private Route getRealRoute(Route route, ServiceInstance serviceInstance) {
        String routeUri = route.getUri().toString();
        String scheme = route.getUri().getScheme();
        if(routeUri.startsWith(SpringCloudGatewayConfiguration.ROUTE_URI_FOR_WEB_SOCKET_HEADER())) {
            //如果RouteUri是lb:ws//开头,让schema赋值为这个头
            scheme = SpringCloudGatewayConfiguration.ROUTE_URI_FOR_WEB_SOCKET_HEADER();
        } else if(routeUri.startsWith(SpringCloudGatewayConfiguration.ROUTE_URI_FOR_HTTP_HEADER())) {
            //如果RouteUri是lb://开头,让schema赋值为这个头
            scheme = SpringCloudGatewayConfiguration.ROUTE_URI_FOR_HTTP_HEADER();
        } else {
            //其余只是加上://
            scheme += "://";
        }
        String uri = scheme + serviceInstance.getApplicationName();
        if(StringUtils.isNotBlank(serviceInstance.getInstance())) {
            uri = scheme + SpringCloudGatewayConfiguration.mergeServiceInstance(serviceInstance);
        }
        //serviceInstance.getInstance()如果为空,uri就等于scheme + applicationName
        //否则就是schema + applicationName和ip端口的merge
        return Route.async().id(route.getId()).filters(route.getFilters()).order(route.getOrder())
                .uri(uri).asyncPredicate(route.getPredicate()).build();
    }
//    @Override
//    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
//        AbstractServerHttpRequest request = (AbstractServerHttpRequest) exchange.getRequest();
//        ServerHttpResponse response = exchange.getResponse();
//        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
//        BaseGatewayContext gatewayContext = getBaseGatewayContext(exchange, route);
//
//        DataBufferFactory bufferFactory = response.bufferFactory();
//        if(((SpringCloudGatewayHttpRequest)gatewayContext.getRequest()).isRequestBodyAutowired()) {
//            ServerHttpRequestDecorator decorator = new ServerHttpRequestDecorator(request) {
//                @Override
//                public Flux<DataBuffer> getBody() {
//                    if(StringUtils.isBlank(gatewayContext.getRequest().getRequestBody())) return Flux.empty();
//                    return Flux.just(bufferFactory.wrap(gatewayContext.getRequest().getRequestBody().getBytes(StandardCharsets.UTF_8)));
//                }
//            };
//            return chain.filter(exchange.mutate().request(decorator).build());
//        } else {
//            return chain.filter(exchange);
//        }
//    }

    private Mono<Void> gatewayDeal(ServerWebExchange exchange, GatewayFilterChain chain, BaseGatewayContext gatewayContext) {
        SpringCloudGatewayHttpResponse gatewayHttpResponse = (SpringCloudGatewayHttpResponse) gatewayContext.getResponse();
        //鉴权验证用户
        if(!SecurityFilter.doFilter(gatewayContext)) {
            //如果没有通过用户验证,gatewayContext中的responsey已经将ResponseMono封装好了(鉴权中调用了sendResponse)
            return gatewayHttpResponse.getResponseMono();
        } else if(gatewayContext.isWebSocketRequest()) {
            //不走entrance的ws请求,直接返回
            return chain.filter(exchange);
        }
        ServiceInstance serviceInstance;
        try {
            //parser从request url中获取到ServiceInstance对象
            //如果ws出问题,entrance走http的话,这里也是需要解析的
            parser.parse(gatewayContext);
            if(gatewayHttpResponse.isCommitted()) {
                //无法parse的时候,这里response早已有ResponseMono的值,这时候直接返回即可
                return gatewayHttpResponse.getResponseMono();
            }
            serviceInstance = router.route(gatewayContext);
        } catch (Throwable t) {
            warn("", t);
            Message message = Message.error(t)
                    .$less$less(gatewayContext.getRequest().getRequestURI());
            if(!gatewayContext.isWebSocketRequest()) gatewayHttpResponse.write(Message.response(message));
            else gatewayHttpResponse.writeWebSocket(Message.response(message));
            gatewayHttpResponse.sendResponse();
            return gatewayHttpResponse.getResponseMono();
        }
        if(gatewayHttpResponse.isCommitted()) {
            return gatewayHttpResponse.getResponseMono();
        }
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        if(serviceInstance != null) {
            Route realRoute = getRealRoute(route, serviceInstance);
            exchange.getAttributes().put(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR, realRoute);
        } else {
            RouteDefinition realRd = null;
            String proxyId = gatewayContext.getGatewayRoute().getParams().get("proxyId");
            for(RouteDefinition rd : gatewayProperties.getRoutes()){
                if((realRd == null && rd.getId().equals("dws")) ||
                        (rd.getId().equals(proxyId))){
                    realRd = rd;
                }
            }
            String uri = realRd.getUri().toString();
            if(uri != null){
                uri = uri + StringUtils.replace(exchange.getRequest().getPath().value(), "/" + realRd.getId() + "/", "");
                info("Proxy to " + uri);
                Route realRoute = Route.async().id(route.getId()).filters(route.getFilters()).order(route.getOrder())
                        .uri(uri).asyncPredicate(route.getPredicate()).build();
                exchange.getAttributes().put(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR, realRoute);
            }
        }
        ServerHttpRequest.Builder builder = exchange.getRequest().mutate().headers(SpringCloudHttpUtils::addIgnoreTimeoutSignal);
        if(!((SpringCloudGatewayHttpRequest) gatewayContext.getRequest()).getAddCookies().isEmpty()) {
            builder.headers(httpHeaders -> {
                SpringCloudHttpUtils.addCookies(httpHeaders, ((SpringCloudGatewayHttpRequest) gatewayContext.getRequest()).getAddCookies());
            });
        }
        return chain.filter(exchange.mutate().request(builder.build()).build());
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        //请求由RouteLocator转进来这里
        AbstractServerHttpRequest request = (AbstractServerHttpRequest) exchange.getRequest();
        //attribute可能是放一些属性的地方,类似requst中的请求域,key,value形式
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        BaseGatewayContext gatewayContext = getBaseGatewayContext(exchange, route);
        //如果是websocket请求,而且包含
        //这里parser的判断逻辑是,如果是/api/rest_j/version/user请求,就直接true,否则直接调用所有parser的实现类的shouldContainRequestBody方法
        //这里实现由2个(gateway-ujes-support模块中)
        //1.如果是entrance/execute  或则entrance/background
        //2.如果是entrance/kill  log等等..
        if(!gatewayContext.isWebSocketRequest() && parser.shouldContainRequestBody(gatewayContext)) {
            return new DefaultServerRequest(exchange).bodyToMono(String.class).flatMap(requestBody -> {
                ((SpringCloudGatewayHttpRequest)gatewayContext.getRequest()).setRequestBody(requestBody);
                ServerHttpRequestDecorator decorator = new ServerHttpRequestDecorator(request) {
                    @Override
                    public Flux<DataBuffer> getBody() {
                        if(StringUtils.isBlank(requestBody)) return Flux.empty();
                        DataBufferFactory bufferFactory = exchange.getResponse().bufferFactory();
                        return Flux.just(bufferFactory.wrap(requestBody.getBytes(StandardCharsets.UTF_8)));
                    }
                };
                return gatewayDeal(exchange.mutate().request(decorator).build(), chain, gatewayContext);
            });
        } else {
            return gatewayDeal(exchange, chain, gatewayContext);
        }
    }

    @Override
    public int getOrder() {
        return 1;
    }
}
