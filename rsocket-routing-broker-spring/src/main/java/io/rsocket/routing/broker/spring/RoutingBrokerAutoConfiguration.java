/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.routing.broker.spring;

import java.util.stream.Collectors;

import io.rsocket.SocketAcceptor;
import io.rsocket.routing.broker.Broker;
import io.rsocket.routing.broker.acceptor.BrokerSocketAcceptor;
import io.rsocket.routing.broker.config.AbstractBrokerProperties;
import io.rsocket.routing.broker.config.TcpBrokerProperties;
import io.rsocket.routing.broker.config.WebsocketBrokerProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.boot.rsocket.context.RSocketServerBootstrap;
import org.springframework.boot.rsocket.netty.NettyRSocketServer;
import org.springframework.boot.rsocket.netty.NettyRSocketServerFactory;
import org.springframework.boot.rsocket.server.RSocketServer;
import org.springframework.boot.rsocket.server.RSocketServerFactory;
import org.springframework.boot.rsocket.server.ServerRSocketFactoryProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorResourceFactory;

@Configuration
@EnableConfigurationProperties
public class RoutingBrokerAutoConfiguration {

	public static final String BROKER_PREFIX = "io.rsocket.routing.broker";

	@Bean
	public Broker broker() {
		return new Broker();
	}

	@Bean
	@ConditionalOnMissingBean
	ReactorResourceFactory reactorResourceFactory() {
		return new ReactorResourceFactory();
	}

	@Bean
	public BrokerSocketAcceptor brokerSocketAcceptor() {
		return new BrokerSocketAcceptor();
	}

	private static RSocketServerFactory getRSocketServerFactory(ReactorResourceFactory resourceFactory,
			ObjectProvider<ServerRSocketFactoryProcessor> processors, AbstractBrokerProperties properties) {
		NettyRSocketServerFactory factory = new NettyRSocketServerFactory();
		factory.setResourceFactory(resourceFactory);
		factory.setTransport(getTransport(properties));
		PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
		map.from(properties.getAddress()).to(factory::setAddress);
		map.from(properties.getPort()).to(factory::setPort);
		factory.setSocketFactoryProcessors(processors.orderedStream().collect(Collectors
				.toList()));
		return factory;
	}

	private static RSocketServer.Transport getTransport(AbstractBrokerProperties properties) {
		switch (properties.getTransport()) {
		case TCP:
			return RSocketServer.Transport.TCP;
		case WEBSOCKET:
			return RSocketServer.Transport.WEBSOCKET;
		}
		throw new IllegalStateException("Unknown Transport " + properties.getTransport());
	}

	@Configuration
	@ConditionalOnProperty(name = TcpConfiguration.TCP_PREFIX + ".enabled", matchIfMissing = true)
	protected static class TcpConfiguration {

		public static final String TCP_PREFIX = BROKER_PREFIX + ".tcp";

		@Bean
		@ConfigurationProperties(TCP_PREFIX)
		public TcpBrokerProperties tcpBrokerProperties() {
			return new TcpBrokerProperties();
		}

		@Bean
		RSocketServerFactory tcpRSocketServerFactory(TcpBrokerProperties properties, ReactorResourceFactory resourceFactory,
				ObjectProvider<ServerRSocketFactoryProcessor> processors) {
			return getRSocketServerFactory(resourceFactory, processors, properties);
		}

		@Bean
		public RSocketServerBootstrap tcpRSocketServerBootstrap(
				TcpBrokerProperties properties,
				@Qualifier("tcpRSocketServerFactory") RSocketServerFactory tcpRSocketServerFactory,
				BrokerSocketAcceptor brokerSocketAcceptor) {
			return new BrokerRSocketServerBootstrap(properties, tcpRSocketServerFactory, brokerSocketAcceptor);
		}
	}

	@Configuration
	@ConditionalOnProperty(name = WebsocketConfiguration.WEBSOCKET_PREFIX + ".enabled")
	protected static class WebsocketConfiguration {
		public static final String WEBSOCKET_PREFIX = BROKER_PREFIX + ".websocket";
		@Bean
		@ConfigurationProperties(WEBSOCKET_PREFIX)
		public WebsocketBrokerProperties websocketBrokerProperties() {
			return new WebsocketBrokerProperties();
		}

		@Bean
		RSocketServerFactory websocketRSocketServerFactory(WebsocketBrokerProperties properties, ReactorResourceFactory resourceFactory,
				ObjectProvider<ServerRSocketFactoryProcessor> processors) {
			return getRSocketServerFactory(resourceFactory, processors, properties);
		}

		@Bean
		public RSocketServerBootstrap websocketRSocketServerBootstrap(
				WebsocketBrokerProperties properties,
				@Qualifier("websocketRSocketServerFactory") RSocketServerFactory websocketRSocketServerFactory,
				BrokerSocketAcceptor brokerSocketAcceptor) {
			return new BrokerRSocketServerBootstrap(properties, websocketRSocketServerFactory, brokerSocketAcceptor);
		}
	}

	private static class BrokerRSocketServerBootstrap extends RSocketServerBootstrap {

		// purposefully using NettyRSocketServer
		private static final Log logger = LogFactory.getLog(NettyRSocketServer.class);
		private final AbstractBrokerProperties properties;

		public BrokerRSocketServerBootstrap(AbstractBrokerProperties properties, RSocketServerFactory serverFactory, SocketAcceptor socketAcceptor) {
			super(serverFactory, socketAcceptor);
			this.properties = properties;
		}

		@Override
		public void start() {
			logger.info("Netty RSocket starting transport: " + properties.getTransport());
			super.start();
		}
	}

}
