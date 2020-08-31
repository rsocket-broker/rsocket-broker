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

import io.rsocket.routing.common.spring.TransportProperties;
import io.rsocket.routing.common.spring.TransportProperties.HostPortProperties;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.boot.rsocket.netty.NettyRSocketServerFactory;
import org.springframework.boot.rsocket.server.RSocketServer;
import org.springframework.boot.rsocket.server.RSocketServerCustomizer;
import org.springframework.boot.rsocket.server.RSocketServerFactory;
import org.springframework.http.client.reactive.ReactorResourceFactory;

public class DefaultServerTransportFactory implements ServerTransportFactory {

	private final ReactorResourceFactory resourceFactory;
	private final ObjectProvider<RSocketServerCustomizer> processors;

	public DefaultServerTransportFactory(ReactorResourceFactory resourceFactory, ObjectProvider<RSocketServerCustomizer> processors) {
		this.resourceFactory = resourceFactory;
		this.processors = processors;
	}

	@Override
	public boolean supports(TransportProperties properties) {
		return properties.getWebsocket() != null || properties.getTcp() != null;
	}

	@Override
	public RSocketServerFactory create(TransportProperties properties) {
		NettyRSocketServerFactory factory = new NettyRSocketServerFactory();
		factory.setResourceFactory(resourceFactory);
		factory.setTransport(findTransport(properties));
		PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
		HostPortProperties addressPort = findHostAndPort(properties);
		map.from(addressPort.getHostAsAddress()).to(factory::setAddress);
		map.from(addressPort.getPort()).to(factory::setPort);
		factory.setRSocketServerCustomizers(processors.orderedStream().collect(Collectors
				.toList()));
		return factory;
	}


	/**
	 * Find the selected transport in order of precedence: custom, websocket, tcp.
	 * @return the selected transport.
	 */
	private static HostPortProperties findHostAndPort(TransportProperties properties) {
		if (properties.hasCustomTransport()) {
			return null;
		} else if (properties.getWebsocket() != null) {
			return properties.getWebsocket();
		} else if (properties.getTcp() != null) {
			return properties.getTcp();
		}
		throw new IllegalStateException("No valid Transport configured " + properties);
	}

	private static RSocketServer.Transport findTransport(TransportProperties properties) {
		if (properties.getWebsocket() != null) {
			return RSocketServer.Transport.WEBSOCKET;
		} else if (properties.getTcp() != null) {
			return RSocketServer.Transport.TCP;
		}
		throw new IllegalStateException("Unknown Transport " + properties);
	}

}
