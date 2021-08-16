/*
 * Copyright 2020-2021 the original author or authors.
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

package io.rsocket.routing.http.bridge.core;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import io.rsocket.routing.common.spring.ClientTransportFactory;
import io.rsocket.routing.common.spring.TransportProperties;
import io.rsocket.routing.http.bridge.config.RSocketHttpBridgeProperties;
import org.apache.commons.logging.Log;

import org.springframework.beans.factory.ObjectProvider;

import static io.rsocket.routing.http.bridge.core.AbstractHttpRSocketFunction.Transport.WEBSOCKET;
import static org.apache.commons.logging.LogFactory.getLog;

/**
 * Base class for HTTP-RSocket bridge functions.
 *
 * @author Olga Maciaszek-Sharma
 * @since 0.3.0
 */
abstract class AbstractHttpRSocketFunction<I, O> implements Function<I, O> {

	public static final String TRANSPORT_KEY = "transport";
	public static final String PORT_KEY = "port";
	public static final String HOST_KEY = "host";
	public static final String MAPPING_PATH_KEY = "mapping-path";

	protected final Log LOG = getLog(getClass());

	protected RoutingRSocketRequester requester;
	ObjectProvider<ClientTransportFactory> transportFactories;
	protected final RSocketHttpBridgeProperties properties;
	protected final Duration timeout;

	protected AbstractHttpRSocketFunction(RoutingRSocketRequester requester, ObjectProvider<ClientTransportFactory> transportFactories,
			RSocketHttpBridgeProperties properties) {
		this.properties = properties;
		timeout = properties.getTimeout();
		this.transportFactories = transportFactories;
		this.requester = requester;
	}

	protected void logTimeout(String address, String route) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String
					.format("Timeout occurred while retrieving RSocket response from address: %s, route: %s. Response was not retrieved within %s", address, route, timeout));
		}
	}

	protected void logException(Throwable error, String address, String route) {
		if (LOG.isErrorEnabled())
			LOG.error(String
					.format("Exception occurred while retrieving RSocket response from address: %s, route: %s", address, route), error);
	}

	protected TransportProperties buildBroker(String brokerData) {
		String[] brokerDataPair = brokerData.toLowerCase().split(",");
		Map<String, String> brokerDataMap = new HashMap<>();
		Arrays.stream(brokerDataPair)
				.map(pair -> Arrays.asList(pair.split("=")))
				.filter(pair -> pair.size() == 2)
				.forEach(pair -> brokerDataMap.put(pair.get(0), pair.get(1)));
		String transport = brokerDataMap.get(TRANSPORT_KEY);
		TransportProperties broker = new TransportProperties();
		if (WEBSOCKET.name().equalsIgnoreCase(transport)) {
			broker.setWebsocket(buildWebsocketBroker(brokerDataMap));
			return broker;
		}
		broker.setTcp(buildTcpBroker(brokerDataMap));
		return broker;
		// TODO: support custom?
	}

	private TransportProperties.TcpProperties buildTcpBroker(Map<String, String> brokerDataMap) {
		TransportProperties.TcpProperties tcpBroker = new TransportProperties.TcpProperties();
		tcpBroker.setHost(brokerDataMap.get(HOST_KEY));
		tcpBroker.setPort(Integer.valueOf(brokerDataMap.getOrDefault(PORT_KEY, "0")));
		return tcpBroker;
	}

	private TransportProperties.WebsocketProperties buildWebsocketBroker(Map<String, String> brokerDataMap) {
		TransportProperties.WebsocketProperties websocketBroker = new TransportProperties.WebsocketProperties();
		websocketBroker.setHost(brokerDataMap.get(HOST_KEY));
		websocketBroker
				.setPort(Integer.valueOf(brokerDataMap.getOrDefault(PORT_KEY, "0")));
		websocketBroker.setMappingPath(brokerDataMap.get(MAPPING_PATH_KEY));
		return websocketBroker;
	}

	/**
	 * Supported broker transport modes.
	 */
	enum Transport {
		TCP, WEBSOCKET
	}

}
