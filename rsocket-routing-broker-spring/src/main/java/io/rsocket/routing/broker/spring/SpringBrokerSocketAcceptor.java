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

import java.util.Map;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.routing.broker.RSocketIndex;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.broker.acceptor.BrokerSocketAcceptor;
import io.rsocket.routing.broker.locator.RSocketLocator;
import io.rsocket.routing.broker.rsocket.RoutingRSocket;
import io.rsocket.routing.frames.RouteSetup;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Mono;

import org.springframework.messaging.rsocket.MetadataExtractor;
import org.springframework.util.MimeType;

public class SpringBrokerSocketAcceptor extends BrokerSocketAcceptor {

	private static final Log log = LogFactory.getLog(BrokerSocketAcceptor.class);

	private final MetadataExtractor metadataExtractor;
	private final RoutingTable routingTable;
	private final RSocketIndex rSocketIndex;

	public SpringBrokerSocketAcceptor(MetadataExtractor metadataExtractor, RoutingTable routingTable, RSocketIndex rSocketIndex) {
		this.metadataExtractor = metadataExtractor;
		this.routingTable = routingTable;
		this.rSocketIndex = rSocketIndex;
	}

	@Override
	public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
		try {
			Map<String, Object> metadata = this.metadataExtractor.extract(setup,
					MimeType.valueOf(setup.metadataMimeType()));

			if (metadata.containsKey(RouteSetup.METADATA_KEY)) {
				RouteSetup routeSetup = (RouteSetup) metadata
						.get(RouteSetup.METADATA_KEY);

				// TODO: metrics
				// TODO: error on disconnect?
				return Mono.defer(() -> {
					// creates receiving socket
					RSocketLocator rSocketLocator = null; // TODO: rSocketLocator
					RoutingRSocket receivingSocket = new RoutingRSocket(rSocketLocator);

					// TODO: deal with existing connection for routeSetup.routeId

					// TODO: enrich tags with routeId and serviceName
					// update routing table with incoming route.
					routingTable.add(routeSetup);

					// adds sendingSocket to rSocketIndex for later lookup
					rSocketIndex.put(routeSetup.getRouteId(), sendingSocket, routeSetup
							.getTags());

					return Mono.fromSupplier(() -> receivingSocket);
				});
			}
			throw new IllegalStateException(RouteSetup.METADATA_KEY + " not found in " + metadata
					.keySet());
		}
		catch (Exception e) {
			if (log.isErrorEnabled()) {
				log.error("Error accepting setup", e);
			}
			return Mono.error(e);
		}
	}
}
