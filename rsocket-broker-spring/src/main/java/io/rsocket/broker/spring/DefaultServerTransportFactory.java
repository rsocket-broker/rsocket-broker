/*
 * Copyright 2021 the original author or authors.
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

package io.rsocket.broker.spring;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.stream.Collectors;

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
	public boolean supports(URI uri) {
		return isWebsocket(uri) || isTcp(uri);
	}

	private static boolean isTcp(URI uri) {
		return uri.getScheme().equalsIgnoreCase("tcp");
	}

	private static boolean isWebsocket(URI uri) {
		return uri.getScheme().equalsIgnoreCase("ws") || uri.getScheme().equalsIgnoreCase("wss");
	}

	@Override
	public RSocketServerFactory create(URI uri) {
		NettyRSocketServerFactory factory = new NettyRSocketServerFactory();
		factory.setResourceFactory(resourceFactory);
		factory.setTransport(findTransport(uri));
		PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
		map.from(getHostAsAddress(uri)).to(factory::setAddress);
		map.from(uri.getPort()).to(factory::setPort);
		factory.setRSocketServerCustomizers(processors.orderedStream().collect(Collectors
				.toList()));
		return factory;
	}


	private static RSocketServer.Transport findTransport(URI uri) {
		if (isWebsocket(uri)) {
			return RSocketServer.Transport.WEBSOCKET;
		}
		else if (isTcp(uri)) {
			return RSocketServer.Transport.TCP;
		}
		throw new IllegalStateException("Unknown Transport " + uri);
	}

	private static InetAddress getHostAsAddress(URI uri) {
		if (uri == null) {
			return null;
		}
		try {
			return InetAddress.getByName(uri.getHost());
		}
		catch (UnknownHostException ex) {
			throw new IllegalStateException("Unknown host " + uri.getHost(), ex);
		}

	}
}
