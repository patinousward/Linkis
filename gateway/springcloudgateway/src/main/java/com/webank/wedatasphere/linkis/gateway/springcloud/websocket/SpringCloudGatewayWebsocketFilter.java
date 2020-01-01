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

package com.webank.wedatasphere.linkis.gateway.springcloud.websocket;

import com.google.common.base.Function;
import com.webank.wedatasphere.linkis.common.ServiceInstance;
import com.webank.wedatasphere.linkis.gateway.http.GatewayContext;
import com.webank.wedatasphere.linkis.gateway.parser.GatewayParser;
import com.webank.wedatasphere.linkis.gateway.route.GatewayRouter;
import com.webank.wedatasphere.linkis.gateway.security.GatewaySSOUtils;
import com.webank.wedatasphere.linkis.gateway.springcloud.http.SpringCloudGatewayHttpRequest;
import com.webank.wedatasphere.linkis.gateway.springcloud.http.SpringCloudHttpUtils;
import com.webank.wedatasphere.linkis.server.Message;
import com.webank.wedatasphere.linkis.server.socket.controller.ServerEvent;
import com.webank.wedatasphere.linkis.server.socket.controller.SocketServerEvent;
import org.springframework.cloud.client.loadbalancer.LoadBalancerClient;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.WebsocketRoutingFilter;
import org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import org.springframework.web.reactive.socket.server.WebSocketService;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.webank.wedatasphere.linkis.gateway.springcloud.websocket.SpringCloudGatewayWebsocketUtils.*;

/**
 * created by cooperyang on 2019/1/9.
 */
public class SpringCloudGatewayWebsocketFilter implements GlobalFilter, Ordered {
    private WebsocketRoutingFilter websocketRoutingFilter;
    private WebSocketClient webSocketClient;
    private WebSocketService webSocketService;
    private LoadBalancerClient loadBalancer;
    private GatewayParser parser ;
    private GatewayRouter router;

    public SpringCloudGatewayWebsocketFilter(WebsocketRoutingFilter websocketRoutingFilter, WebSocketClient webSocketClient,
                                             WebSocketService webSocketService, LoadBalancerClient loadBalancer,
                                             GatewayParser parser, GatewayRouter router) {
        this.websocketRoutingFilter = websocketRoutingFilter;
        this.webSocketClient = webSocketClient;
        this.webSocketService = webSocketService;
        this.loadBalancer = loadBalancer;
        this.parser = parser;
        this.router = router;
    }

    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ////这一步是模仿WebsocketRoutingFilter中的filter方法(用反射)
        changeSchemeIfIsWebSocketUpgrade(websocketRoutingFilter, exchange);
        //模仿WebsocketRoutingFilter  获取requestUrl
        URI requestUrl = exchange.getRequiredAttribute(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR);
        //获取url中的schema
        String scheme = requestUrl.getScheme();
        //模仿WebsocketRoutingFilter 只是修改了boolean的判断
        if (!ServerWebExchangeUtils.isAlreadyRouted(exchange) && ("ws".equals(scheme) || "wss".equals(scheme))) {
            ServerWebExchangeUtils.setAlreadyRouted(exchange);
            HttpHeaders headers = exchange.getRequest().getHeaders();
            List<String> protocols = headers.get("Sec-WebSocket-Protocol");
            //到目前都是模仿WebsocketRoutingFilter
            if (protocols != null) {
                protocols = (List<String>)protocols.stream().flatMap((header) -> {
                    return Arrays.stream(StringUtils.commaDelimitedListToStringArray(header));
                }).map(String::trim).collect(Collectors.toList());
            }
            //到目前都是模仿WebsocketRoutingFilter
            List<String> collectedProtocols = protocols;
            //new 一个BaseGatewayContext,并将exchange中的request封装为SpringCloudGatewayHttpRequest,response则直接new WebsocketGatewayHttpResponse,没有封装
            GatewayContext gatewayContext = getGatewayContext(exchange);
            //return 也是模仿WebsocketRoutingFilter的
            //new 一个WebSocketHandler的匿名实现类,重写方法
            //这里再此展示了匿名实现类的一些用法,可以将外部参数直接传递进去方法
            return this.webSocketService.handleRequest(exchange, new WebSocketHandler() {
                public Mono<Void> handle(WebSocketSession webClientSocketSession) {
                    //封装webClientSocketSession,并且将对象的引用放入缓存中
                    GatewayWebSocketSessionConnection gatewayWebSocketSession = getGatewayWebSocketSessionConnection(GatewaySSOUtils.getLoginUsername(gatewayContext), webClientSocketSession);
                    //创建一个FluxSinkListner的匿名实现对象(这个类是自己定义的)
                    FluxSinkListener fluxSinkListener = new FluxSinkListener<WebSocketMessage>(){
                        private FluxSink<WebSocketMessage> fluxSink = null;
                        @Override
                        public void setFluxSink(FluxSink<WebSocketMessage> fluxSink) {
                            this.fluxSink = fluxSink;
                        }
                        @Override
                        public void next(WebSocketMessage webSocketMessage) {
                            //输出的时候更新下用户访问时间,就是websocket推送的时候更新
                            //保证脚本执行的过程中,不会因为登陆等问题跳到登陆页面
                            if(fluxSink != null) fluxSink.next(webSocketMessage);
                            GatewaySSOUtils.updateLastAccessTime(gatewayContext);
                        }
                        @Override
                        public void complete() {
                            if(fluxSink != null) fluxSink.complete();
                        }
                    };
                    //创建一个Flux,并且将sink对象放入fluxSinkListener中，当listener调用next方法的时候，sink的next方法就会被调用
                    //当然需要先subscribe，否则create中的函数不会被调用
                    Flux<WebSocketMessage> receives = Flux.create(sink -> {
                        fluxSinkListener.setFluxSink(sink);
                    });
                    //receive()方法,转化为WebSocketMessage(Flux<WebSocketFrame> -->Flux<WebSocketMessage> )
                    gatewayWebSocketSession.receive().doOnNext(WebSocketMessage::retain).map(t -> {
                        String user;
                        try {
                            user = GatewaySSOUtils.getLoginUsername(gatewayContext);
                        } catch (Throwable e) {
                            if(gatewayWebSocketSession.isAlive()) {
                                String message = Message.response(Message.noLogin(e.getMessage()).$less$less(gatewayContext.getRequest().getRequestURI()));;
                                fluxSinkListener.next(getWebSocketMessage(gatewayWebSocketSession.bufferFactory(), message));
                            }
                            return gatewayWebSocketSession.close();
                        }
                        //public enum Type { TEXT, BINARY, PING, PONG }
                        //维持ping pong,防止websocket断掉
                        if(t.getType() == WebSocketMessage.Type.PING || t.getType() == WebSocketMessage.Type.PONG) {
                            WebSocketMessage pingMsg = new WebSocketMessage(WebSocketMessage.Type.PING, t.getPayload());
                            gatewayWebSocketSession.heartbeat(pingMsg);
                            return sendMsg(exchange, gatewayWebSocketSession, pingMsg);
                        }
                        //这里payload 的概念好像缓冲区数据?
                        String json = t.getPayloadAsText();
                        t.release();
                        //将前台的数据反序列化为一个ServerEvent对象
                        ServerEvent serverEvent = SocketServerEvent.getServerEvent(json);
                        //ServerEvent对象 中的data数据()放入请求体,method(entrance/execute或则entrance/background,只有这2个)放入requesturi中
                        ((SpringCloudGatewayHttpRequest) gatewayContext.getRequest()).setRequestBody(SocketServerEvent.getMessageData(serverEvent));
                        ((SpringCloudGatewayHttpRequest) gatewayContext.getRequest()).setRequestURI(serverEvent.getMethod());
                        //和http一样,通过url生成相应的ServiceInstance对象
                        parser.parse(gatewayContext);
                        if (gatewayContext.getResponse().isCommitted()) {
                            return sendMsg(exchange, gatewayWebSocketSession, ((WebsocketGatewayHttpResponse) gatewayContext.getResponse()).getWebSocketMsg());
                        }
                        //router 补充ServiceInstance信息,含ip端口
                        ServiceInstance serviceInstance = router.route(gatewayContext);
                        if (gatewayContext.getResponse().isCommitted()) {
                            return sendMsg(exchange, gatewayWebSocketSession, ((WebsocketGatewayHttpResponse) gatewayContext.getResponse()).getWebSocketMsg());
                        }
                        //从gatewayWebSocketSession中获取存活的代理的WebSocketSession,不存活得从缓存中移除
                        //一个gatewayWebSocketSession对象中有个缓存保存ProxyWebSocketSession的集合
                        WebSocketSession webSocketProxySession = getProxyWebSocketSession(gatewayWebSocketSession, serviceInstance);
                        if (webSocketProxySession != null) {
                            return sendMsg(exchange, webSocketProxySession, json);
                        } else {
                            //一般来说,刚进行ws连接的话,上面获取的webSocketProxySession是null
                            URI uri = exchange.getRequest().getURI();
                            //看下uri是否经过了url编码
                            Boolean encoded = ServerWebExchangeUtils.containsEncodedParts(uri);
                            String host;
                            int port;
                            //从Instance中获取ip和端口号,并重新封装requestURI
                            //之前的requestURI  的ip的端口是gateway的,这里需要封装为entrance那边的ip和端口
                            //转发的原因是因为springcloud gateway 没法进行websocket的转发?只能提供gateway和浏览器的ws连接
                            if (StringUtils.isEmpty(serviceInstance.getInstance())) {
                                //如果instance为空,从loadbalance中获取service(一般不会走这步?因为上面router中一般会封装了,除非新加入服务之类的)
                                org.springframework.cloud.client.ServiceInstance service = loadBalancer.choose(serviceInstance.getApplicationName());
                                host = service.getHost();
                                port = service.getPort();
                            } else {
                                String[] instanceInfo = serviceInstance.getInstance().split(":");
                                host = instanceInfo[0];
                                port = Integer.parseInt(instanceInfo[1]);
                            }
                            //这里uri应该是ws://gatewayip:port/ws/api/entrance/connect
                            //  其中/ws开头经过nginx直接转发到gateway进入filter
                            //而http请求  /api开头通过nginx转发也进入gateway的filter
                            //gateway可以转发ws请求,但是这个ws地址可能是不同的,对于linkis这种接口都一样的
                            //wds.linkis.server.socket.uri 中配置jetty的websoket path,Gatwickway拦截后进入websocket的servlet
                            URI requestURI = UriComponentsBuilder.fromUri(requestUrl).host(host).port(port).build(encoded).toUri();
                            //模仿WebsocketRoutingFilter进行httpheader的过滤
                            HttpHeaders filtered = HttpHeadersFilter.filterRequest(getHeadersFilters(websocketRoutingFilter), exchange);
                            //封装忽略超时的cookies 到header中,避免ws传输中断掉
                            SpringCloudHttpUtils.addIgnoreTimeoutSignal(filtered);
                            //这里返回的是map中进行转化的内容   WebSocketMessage--->Mono<Void>
                            //websocketClient可以对比httpclient联想,就是发送websocket请求的,这里用的注入实例是ReactorNettyWebSocketClient
                            return webSocketClient.execute(requestURI, filtered, new WebSocketHandler() {
                                public Mono<Void> handle(WebSocketSession proxySession) {
                                    //gatewayWebSocketSession的ProxyWebSocketSession缓存中添加此次代理ws的对象
                                    setProxyWebSocketSession(user, serviceInstance, gatewayWebSocketSession, proxySession);
                                    //将获取的数据重新封装为 Mono<Void>,使用proxySession进行发送数据
                                    Mono<Void> proxySessionSend = sendMsg(exchange, proxySession, json);
                                    proxySessionSend.subscribe();
                                    return getProxyWebSocketSession(gatewayWebSocketSession, serviceInstance).receive()
                                            //then 是flux<> 转Mono<Void>的方法
                                            //fluxSinkListener::next将数据直接流向了receives中
                                            .doOnNext(WebSocketMessage::retain).doOnNext(fluxSinkListener::next).then();
                                }

                                public List<String> getSubProtocols() {
                                    return collectedProtocols;
                                }
                            });
                        }
                    }).doOnComplete(fluxSinkListener::complete).doOnNext(Mono::subscribe).subscribe();
                    //通过map的循环遍历，直接就将原来websocketsession中的数据转化url后直接放入receives中，通过send方法进行发送

                    //这里才是重写方法的返回
                    return gatewayWebSocketSession.send(receives);
                }
                //这个看WebsocketRoutingFilter知道,protocols就是92-93行中获取到的protocols
                public List<String> getSubProtocols() {
                    return collectedProtocols;
                }
            });
        } else {
            return chain.filter(exchange);
        }
    }

    public int getOrder() {
        return websocketRoutingFilter.getOrder() - 1; 
    }

    interface FluxSinkListener<T> {
        void setFluxSink(FluxSink<T> fluxSink);
        void next(T t);
        void complete();
    }
}
