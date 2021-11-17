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

package io.rsocket.broker.spring.cluster;

import java.io.Closeable;
import java.util.function.Predicate;

import io.rsocket.broker.RoutingTable;
import io.rsocket.broker.spring.BrokerProperties;
import io.rsocket.broker.spring.cluster.AbstractConnections.BrokerInfoEntry;
import io.rsocket.broker.frames.BrokerInfo;
import io.rsocket.broker.frames.RouteJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.messaging.rsocket.RSocketRequester;

/**
 * When a client connects, this class will relay the RouteJoin to other connected brokers.
 */
public class RouteJoinListener implements Closeable {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Disposable disposable;

	public RouteJoinListener(BrokerProperties properties, RoutingTable routingTable,
			BrokerConnections brokerConnections) {
		// only listen for local RouteJoin events
		Predicate<RouteJoin> predicate = routeJoin -> properties.getBrokerId()
				.equals(routeJoin.getBrokerId());

		disposable = routingTable.joinEvents(predicate)
				.flatMap(routeJoin -> Flux.fromIterable(brokerConnections.entries())
						.flatMap(entry -> sendRouteJoin(entry, routeJoin))).subscribe();
	}

	private Mono<RouteJoin> sendRouteJoin(BrokerInfoEntry<RSocketRequester> entry,
			RouteJoin routeJoin) {
		logger.info("sending RouteJoin {} to {}", routeJoin, entry.getBrokerInfo());
		RSocketRequester requester = entry.getValue();
		return requester.route("cluster.route-join")
				.data(routeJoin)
				.retrieveMono(RouteJoin.class);
	}

	@Override
	public void close() {
		if (!disposable.isDisposed()) {
			disposable.dispose();
		}
	}
}
