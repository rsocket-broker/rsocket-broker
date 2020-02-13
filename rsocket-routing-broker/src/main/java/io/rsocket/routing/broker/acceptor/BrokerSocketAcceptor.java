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

package io.rsocket.routing.broker.acceptor;

import java.util.function.Function;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.routing.broker.RSocketIndex;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.broker.config.BrokerProperties;
import io.rsocket.routing.broker.locator.RSocketLocator;
import io.rsocket.routing.broker.rsocket.RoutingRSocket;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.common.WellKnownKey;
import io.rsocket.routing.frames.RouteJoin;
import io.rsocket.routing.frames.RouteSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class BrokerSocketAcceptor implements SocketAcceptor {
	protected static final Logger logger = LoggerFactory
			.getLogger(BrokerSocketAcceptor.class);

	protected final RoutingTable routingTable;
	protected final RSocketIndex rSocketIndex;
	protected final RSocketLocator rSocketLocator;
	protected final Function<ConnectionSetupPayload, RouteSetup> routeSetupExtractor;
	protected final Function<Payload, Tags> tagsExtractor;
	protected final BrokerProperties properties;

	public BrokerSocketAcceptor(BrokerProperties properties, RoutingTable routingTable,
			RSocketIndex rSocketIndex, RSocketLocator rSocketLocator,
			Function<ConnectionSetupPayload, RouteSetup> routeSetupExtractor,
			Function<Payload, Tags> tagsExtractor) {
		this.routingTable = routingTable;
		this.rSocketIndex = rSocketIndex;
		this.rSocketLocator = rSocketLocator;
		this.routeSetupExtractor = routeSetupExtractor;
		this.tagsExtractor = tagsExtractor;
		this.properties = properties;

		logger.info("Starting Broker {}", properties.getBrokerId());
	}

	@Override
	public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
		try {
			RouteSetup routeSetup = routeSetupExtractor.apply(setup);

			if (routeSetup != null) {
				// TODO: metrics
				// TODO: error on disconnect?
				return Mono.defer(() -> {
					// TODO: deal with existing connection for routeSetup.routeId
					RoutingRSocket receivingSocket = new RoutingRSocket(rSocketLocator, tagsExtractor);

					// create RouteJoin before indexing or RoutingTable
					RouteJoin routeJoin = toRouteJoin(routeSetup);

					// update index before RoutingTable
					// adds sendingSocket to rSocketIndex for later lookup
					rSocketIndex.put(routeJoin.getRouteId(), sendingSocket, routeJoin
							.getTags());

					// update routing table with incoming route.
					routingTable.add(routeJoin);

					return Mono.fromSupplier(() -> receivingSocket);
				});
			}

			throw new IllegalStateException("RouteSetup not found in metadata");
		}
		catch (Exception e) {
			logger.error("Error accepting setup", e);
			return Mono.error(e);

		}
	}

	/**
	 * Creates RouteJoin with current brokerId and enriches tags with routeId and serviceName.
 	 */
	private RouteJoin toRouteJoin(RouteSetup routeSetup) {
		return RouteJoin.builder()
				.brokerId(properties.getBrokerId())
				.routeId(routeSetup.getRouteId())
				.serviceName(routeSetup.getServiceName())
				.with(routeSetup.getTags())
				.with(WellKnownKey.ROUTE_ID, routeSetup.getRouteId().toString())
				.with(WellKnownKey.SERVICE_NAME, routeSetup.getServiceName())
				.build();
	}
}
