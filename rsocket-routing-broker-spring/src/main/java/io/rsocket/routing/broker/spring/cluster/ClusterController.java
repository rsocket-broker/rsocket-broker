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

package io.rsocket.routing.broker.spring.cluster;

import java.time.Duration;

import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.broker.config.BrokerProperties;
import io.rsocket.routing.frames.BrokerInfo;
import io.rsocket.routing.frames.RouteJoin;
import io.rsocket.routing.frames.RoutingFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.annotation.ConnectMapping;
import org.springframework.stereotype.Controller;

/**
 * Handles inter-broker communication.
 */
@Controller
public class ClusterController {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final BrokerProperties properties;
	private final BrokerConnections brokerConnections;
	private final RoutingTable routingTable;

	private final FluxProcessor<BrokerInfo, BrokerInfo> connectEvents =
			DirectProcessor.<BrokerInfo>create().serialize();

	public ClusterController(BrokerProperties properties, BrokerConnections brokerConnections, RoutingTable routingTable) {
		this.properties = properties;
		this.brokerConnections = brokerConnections;
		this.routingTable = routingTable;

		// subscribe to connect events so that a return BrokerInfo call is delayed
		// This allows broker to maintain a single connection, but allows other broker
		// to call brokerInfo() to get this broker's id and add it to BrokerConnections.
		// TODO: configurable delay
		connectEvents.delayElements(Duration.ofSeconds(1))
				.flatMap(brokerInfo -> {
					RSocketRequester requester = brokerConnections.get(brokerInfo);
					return sendBrokerInfo(requester, brokerInfo);
				}).subscribe();
	}

	@ConnectMapping
	public Mono<Void> onConnect(RoutingFrame routingFrame, RSocketRequester rSocketRequester) {
		// FIXME: hack
		if (!(routingFrame instanceof BrokerInfo)) {
			return Mono.empty();
		}
		BrokerInfo brokerInfo = (BrokerInfo) routingFrame;
		if (brokerInfo.getBrokerId().equals(properties.getBrokerId())) {
			//TODO: weird case I wonder if I can avoid
			return Mono.empty();
		}
		logger.info("received connection from {}", brokerInfo);

		if (brokerConnections.contains(brokerInfo)) {
			// reject duplicate connections
			return Mono.error(new RejectedSetupException("Duplicate connection from " + brokerInfo));
		}

		// store broker info
		brokerConnections.put(brokerInfo, rSocketRequester);

		// send a connectEvent
		connectEvents.onNext(brokerInfo);
		return Mono.empty();
	}

	@MessageMapping("cluster.broker-info")
	public Mono<BrokerInfo> brokerInfo(BrokerInfo brokerInfo, RSocketRequester rSocketRequester) {
		logger.info("received brokerInfo from {}", brokerInfo);

		// if brokerConnections has connections
		if (brokerConnections.contains(brokerInfo)) {
			logger.debug("connection for broker already exists {}", brokerInfo);
			// we can now accept RouteJoin from this broker
			// TODO: add flag to RoutingTable for this broker
			return Mono.just(BrokerInfo.from(properties.getBrokerId()).build());
		}

		// else store broker info
		brokerConnections.put(brokerInfo, rSocketRequester);

		// send BrokerInfo back
		return sendBrokerInfo(rSocketRequester, brokerInfo);
	}

	private Mono<BrokerInfo> sendBrokerInfo(RSocketRequester rSocketRequester, BrokerInfo brokerInfo) {
		BrokerInfo localBrokerInfo = BrokerInfo.from(properties.getBrokerId()).build();
		return rSocketRequester.route("cluster.broker-info")
				.data(localBrokerInfo)
				.retrieveMono(BrokerInfo.class)
				.map(bi -> localBrokerInfo);
	}

	@MessageMapping("cluster.route-join")
	private Mono<RouteJoin> routeJoin(RouteJoin routeJoin) {
		logger.info("received RouteJoin {}", routeJoin);

		BrokerInfo brokerInfo = BrokerInfo.from(routeJoin.getBrokerId()).build();
		if (!brokerConnections.contains(brokerInfo)) {
			// attempting to add a route for a broker with no connection.
			return Mono.error(new ApplicationErrorException("No connection for broker " + brokerInfo));
		}

		routingTable.add(routeJoin);

		return Mono.just(routeJoin);
	}

	@MessageMapping("hello")
	public Mono<String> hello(String name) {
		return Mono.just("Hello " + name);
	}

}
